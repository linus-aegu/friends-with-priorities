// OpenAI-compatible proxy that shares one GPU between friends.
// The model name picks the lane: "llama-70b:nitro", "llama-70b" or "llama-70b:flex".

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import http from 'node:http'
import { text } from 'node:stream/consumers'
import { setTimeout as sleep } from 'node:timers/promises'

const SLOTS = 4 // parallel requests on the GPU

// Key order is the priority. max: parallel requests per lane, price: cost multiplier.
const LANES = {
  nitro:  { max: 4, price: 2.0, queue: [], running: 0 },
  normal: { max: 3, price: 1.0, queue: [], running: 0 },
  flex:   { max: 1, price: 0.3, queue: [], running: 0 },
}

const MODELS = ['llama-70b']
const FRIENDS = new Map([
  ['sk-alice', 'alice'],
  ['sk-bob', 'bob'],
  ['sk-carol', 'carol'],
])

const MODE = process.argv[2] // 'serve', 'bill' or undefined for the demo
const USAGE_FILE = new URL('usage.json', import.meta.url)
const KEEP_USAGE = MODE === 'serve' || MODE === 'bill' // the demo doesn't read or write usage.json
// Weighted tokens per month and friend, e.g. { "2026-10": { "alice": 1300 } }
const usage = KEEP_USAGE && existsSync(USAGE_FILE) ? JSON.parse(readFileSync(USAGE_FILE, 'utf8')) : {}
const busy = () => Object.values(LANES).reduce((sum, lane) => sum + lane.running, 0)

function schedule(lane, signal, job) {
  return new Promise((resolve, reject) => {
    const entry = { job, resolve, reject }
    lane.queue.push(entry)
    // Client gone while queued: remove the request. Running jobs handle the signal themselves.
    signal.addEventListener('abort', () => {
      const i = lane.queue.indexOf(entry)
      if (i === -1) return
      lane.queue.splice(i, 1)
      reject(signal.reason)
    }, { once: true })
    next()
  })
}

// Called whenever a request arrives or finishes. Fills free slots, highest priority first.
function next() {
  for (const lane of Object.values(LANES)) {
    while (busy() < SLOTS && lane.running < lane.max && lane.queue.length) {
      const { job, resolve, reject } = lane.queue.shift()
      lane.running++
      job().then(resolve, reject).finally(() => {
        lane.running--
        next()
      })
    }
    if (lane.queue.length) break // lower lanes wait while a higher lane has a queue
  }
}

// "llama-70b:flex" becomes model "llama-70b" in the flex lane. Only lane tags are stripped.
function parseModel(name) {
  for (const [tag, lane] of Object.entries(LANES)) {
    if (name.endsWith(`:${tag}`)) return { model: name.slice(0, -tag.length - 1), lane }
  }
  return { model: name, lane: LANES.normal }
}

// POST /v1/chat/completions. The body is forwarded as is, only the tag is removed from the model.
async function chatCompletion(friend, body, res, signal) {
  signal.throwIfAborted()
  const { model, lane } = parseModel(body.model)
  if (!MODELS.includes(model)) throw httpError(400, `Unknown model "${model}", see GET /v1/models`)
  const upstreamBody = { ...body, model }
  // Streams only include token usage when include_usage is set.
  if (body.stream) upstreamBody.stream_options = { ...body.stream_options, include_usage: true }

  let ok = false
  let tail = '' // end of the response, where the usage is
  let events = 0
  try {
    // The slot stays taken until the backend has finished.
    await schedule(lane, signal, async () => {
      const upstream = await gpu(upstreamBody, signal)
      ok = upstream.ok
      const type = upstream.headers.get('content-type')
      res.writeHead(upstream.status, type ? { 'content-type': type } : {})
      for await (const chunk of upstream.body.pipeThrough(new TextDecoderStream())) {
        res.write(chunk)
        tail = (tail + chunk).slice(-2000)
        events += chunk.split('data: ').length - 1
      }
      res.end()
    })
  } catch (err) {
    // Client hung up mid-stream: bill about one token per event sent so far.
    if (signal.aborted && ok && events) charge(friend, lane, events)
    throw err
  }
  if (!ok) return // backend error, already forwarded to the client

  const tokens = Number([...tail.matchAll(/"total_tokens":\s*(\d+)/g)].at(-1)?.[1])
  if (!tokens) throw new Error(`Backend sent no token usage, can't bill ${friend}`)
  charge(friend, lane, tokens)
}

const thisMonth = () => new Date().toISOString().slice(0, 7) // YYYY-MM in UTC

function charge(friend, lane, tokens) {
  const month = thisMonth()
  usage[month] ??= {}
  usage[month][friend] = (usage[month][friend] ?? 0) + tokens * lane.price
  if (KEEP_USAGE) writeFileSync(USAGE_FILE, JSON.stringify(usage, null, 2))
}

// Splits the bill by usage. Shares are in cents and add up to the bill exactly.
function splitBill(month, bill) {
  if (!usage[month]) throw new Error(`No usage recorded for ${month}`)
  const used = Object.entries(usage[month])
  const total = used.reduce((sum, [, tokens]) => sum + tokens, 0)
  const billCents = Math.round(bill * 100)
  const shares = used.map(([friend, tokens]) => {
    const exact = (billCents * tokens) / total
    return { friend, cents: Math.floor(exact), rest: exact % 1 }
  })
  // Round down, then give the leftover cents to the largest remainders.
  const leftover = billCents - shares.reduce((sum, s) => sum + s.cents, 0)
  shares.sort((a, b) => b.rest - a.rest)
  return Object.fromEntries(shares.map((s, i) => [s.friend, (s.cents + (i < leftover ? 1 : 0)) / 100]))
}

// Fake backend, each request takes about 1s. To use a real one (vLLM, llama.cpp, ollama):
//   return fetch('http://gpu:8000/v1/chat/completions', {
//     method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal,
//   })
async function gpu(body, signal) {
  if (!body.stream) {
    await sleep(1000, null, { signal })
    return Response.json({
      object: 'chat.completion',
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: 'hi there!' }, finish_reason: 'stop' }],
      usage: { total_tokens: 1000 },
    })
  }
  const event = (data) => `data: ${JSON.stringify({ object: 'chat.completion.chunk', model: body.model, ...data })}\n\n`
  async function* sse() {
    for (const word of ['hi', ' there', '!']) {
      await sleep(333, null, { signal })
      yield event({ choices: [{ index: 0, delta: { content: word } }] })
    }
    yield event({ choices: [], usage: { total_tokens: 1000 } })
    yield 'data: [DONE]\n\n'
  }
  const stream = ReadableStream.from(sse()).pipeThrough(new TextEncoderStream())
  return new Response(stream, { headers: { 'content-type': 'text/event-stream' } })
}

const httpError = (status, message) => Object.assign(new Error(message), { status })
const reply = (res, status, data) =>
  res.writeHead(status, { 'content-type': 'application/json' }).end(JSON.stringify(data))

async function handle(req, res, signal) {
  const friend = FRIENDS.get(req.headers.authorization?.replace('Bearer ', ''))
  if (!friend) throw httpError(401, 'Unknown API key')
  if (req.method === 'GET' && req.url === '/v1/models') {
    const ids = MODELS.flatMap((m) => [m, ...Object.keys(LANES).map((tag) => `${m}:${tag}`)])
    return reply(res, 200, { object: 'list', data: ids.map((id) => ({ id, object: 'model' })) })
  }
  if (req.method !== 'POST' || req.url !== '/v1/chat/completions') {
    throw httpError(404, `No route for ${req.method} ${req.url}`)
  }

  const raw = await text(req)
  let body
  try {
    body = JSON.parse(raw)
  } catch {
    throw httpError(400, 'Body is not valid JSON')
  }
  if (typeof body?.model !== 'string') throw httpError(400, 'Missing "model"')

  await chatCompletion(friend, body, res, signal)
}

if (MODE === 'serve') {
  // OPENAI_BASE_URL=http://localhost:8080/v1 OPENAI_API_KEY=sk-alice
  http.createServer((req, res) => {
    const hangup = new AbortController()
    // Connection closed before the response finished: the client is gone.
    res.on('close', () => {
      if (!res.writableFinished) hangup.abort()
    })

    handle(req, res, hangup.signal).catch((err) => {
      if (hangup.signal.aborted) return console.log(`${req.method} ${req.url} cancelled by client`)
      console.error(err.status ? `${err.status} ${err.message}` : err) // stack trace only for unexpected errors
      if (!res.headersSent) reply(res, err.status ?? 500, { error: { message: err.message } })
      else if (!res.writableEnded) res.destroy() // headers already sent, so cut the connection
    })
  }).listen(8080)
} else if (MODE === 'bill') {
  // node priorities.mjs bill 2026-10 30
  const [month, euros] = process.argv.slice(3)
  if (!/^\d{4}-\d{2}$/.test(month ?? '') || !(Number(euros) > 0)) {
    throw new Error('Usage: node priorities.mjs bill <YYYY-MM> <euros>')
  }
  console.log(splitBill(month, Number(euros)))
} else {
  // Demo with the fake backend. One queued and one running request get cancelled.
  const t0 = Date.now()
  const log = (...msg) => console.log(`${((Date.now() - t0) / 1000).toFixed(1)}s `, ...msg)
  const sink = { writeHead() {}, write() {}, end() {} } // stands in for the HTTP response
  const times = (n, fn) => Array.from({ length: n }, fn)
  const ask = (friend, model, { stream = false, cancelAfter } = {}) => {
    const hangup = new AbortController()
    if (cancelAfter) setTimeout(() => hangup.abort(), cancelAfter)
    const body = { model, stream, messages: [{ role: 'user', content: 'hi' }] }
    return chatCompletion(friend, body, sink, hangup.signal)
      .then(() => log(friend.padEnd(6), model, stream ? '(stream)' : ''))
      .catch((err) => log(friend.padEnd(6), model, `failed: ${err.message}`))
  }

  await Promise.all([
    ...times(3, () => ask('alice', 'llama-70b:flex')),
    ask('alice', 'llama-70b:flex', { cancelAfter: 200 }),
    ask('bob', 'llama-70b', { stream: true, cancelAfter: 400 }),
    ...times(5, () => ask('bob', 'llama-70b', { stream: true })),
    sleep(500).then(() => Promise.all(times(2, () => ask('carol', 'llama-70b:nitro')))),
  ])
  console.log(`\n€30 server bill for ${thisMonth()}:`, splitBill(thisMonth(), 30))
}
