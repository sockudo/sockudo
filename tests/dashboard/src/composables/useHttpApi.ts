import { useDashboardStore } from '../stores/dashboard'

async function hmacSha256(key: string, message: string): Promise<string> {
  const enc = new TextEncoder()
  const cryptoKey = await crypto.subtle.importKey('raw', enc.encode(key), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
  const sig = await crypto.subtle.sign('HMAC', cryptoKey, enc.encode(message))
  return Array.from(new Uint8Array(sig)).map(b => b.toString(16).padStart(2, '0')).join('')
}

// Pure JS MD5 — crypto.subtle doesn't support MD5 in browsers
function md5(input: string): string {
  const bytes = new TextEncoder().encode(input)

  function toWord(b: Uint8Array, i: number) {
    return b[i] | (b[i + 1] << 8) | (b[i + 2] << 16) | (b[i + 3] << 24)
  }

  // Pad message
  const bitLen = bytes.length * 8
  const padLen = bytes.length + 1 + ((55 - bytes.length % 64 + 64) % 64) + 8
  const padded = new Uint8Array(padLen)
  padded.set(bytes)
  padded[bytes.length] = 0x80
  const view = new DataView(padded.buffer)
  view.setUint32(padLen - 8, bitLen >>> 0, true)
  view.setUint32(padLen - 4, (bitLen / 0x100000000) >>> 0, true)

  const S = [
    7,12,17,22,7,12,17,22,7,12,17,22,7,12,17,22,
    5,9,14,20,5,9,14,20,5,9,14,20,5,9,14,20,
    4,11,16,23,4,11,16,23,4,11,16,23,4,11,16,23,
    6,10,15,21,6,10,15,21,6,10,15,21,6,10,15,21,
  ]
  const K = Array.from({ length: 64 }, (_, i) => (Math.floor(Math.abs(Math.sin(i + 1)) * 0x100000000)) >>> 0)

  let a0 = 0x67452301 >>> 0
  let b0 = 0xefcdab89 >>> 0
  let c0 = 0x98badcfe >>> 0
  let d0 = 0x10325476 >>> 0

  for (let offset = 0; offset < padLen; offset += 64) {
    const M = Array.from({ length: 16 }, (_, i) => toWord(padded, offset + i * 4) >>> 0)
    let A = a0, B = b0, C = c0, D = d0

    for (let i = 0; i < 64; i++) {
      let F: number, g: number
      if (i < 16) { F = (B & C) | (~B & D); g = i }
      else if (i < 32) { F = (D & B) | (~D & C); g = (5 * i + 1) % 16 }
      else if (i < 48) { F = B ^ C ^ D; g = (3 * i + 5) % 16 }
      else { F = C ^ (B | ~D); g = (7 * i) % 16 }

      F = ((F >>> 0) + A + K[i] + M[g]) >>> 0
      A = D
      D = C
      C = B
      const rot = ((F << S[i]) | (F >>> (32 - S[i]))) >>> 0
      B = (B + rot) >>> 0
    }

    a0 = (a0 + A) >>> 0
    b0 = (b0 + B) >>> 0
    c0 = (c0 + C) >>> 0
    d0 = (d0 + D) >>> 0
  }

  const result = new Uint8Array(16)
  const rv = new DataView(result.buffer)
  rv.setUint32(0, a0, true)
  rv.setUint32(4, b0, true)
  rv.setUint32(8, c0, true)
  rv.setUint32(12, d0, true)
  return Array.from(result).map(b => b.toString(16).padStart(2, '0')).join('')
}

export function useHttpApi() {
  const store = useDashboardStore()

  function baseUrl() {
    const proto = store.config.useTLS ? 'https' : 'http'
    return `${proto}://${store.config.host}:${store.config.port}`
  }

  async function signRequest(method: string, path: string, queryParams: Record<string, string> = {}, body?: string) {
    const params: Record<string, string> = {
      auth_key: store.config.appKey,
      auth_timestamp: Math.floor(Date.now() / 1000).toString(),
      auth_version: '1.0',
      ...queryParams,
    }

    if (body) {
      params.body_md5 = md5(body)
    }

    const qs = Object.keys(params).sort().map(k => `${k}=${params[k]}`).join('&')
    params.auth_signature = await hmacSha256(store.config.appSecret, `${method}\n${path}\n${qs}`)
    return params
  }

  async function apiRequest(method: string, path: string, body?: unknown, queryParams: Record<string, string> = {}) {
    const bodyStr = body ? JSON.stringify(body) : undefined
    const fullPath = `/apps/${store.config.appId}${path}`
    const params = await signRequest(method, fullPath, queryParams, bodyStr)

    const url = new URL(`${baseUrl()}${fullPath}`)
    for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v)

    store.addEvent({ direction: 'out', event: `HTTP ${method} ${path}`, data: body || queryParams })

    try {
      const res = await fetch(url.toString(), {
        method,
        headers: body ? { 'Content-Type': 'application/json' } : undefined,
        body: bodyStr,
      })
      const data = await res.json().catch(() => res.text())
      store.addEvent({ direction: 'in', event: `HTTP ${res.status} ${path}`, data })
      return { status: res.status, data }
    } catch (err: any) {
      store.addEvent({ direction: 'system', event: `HTTP Error ${path}`, data: { error: err.message } })
      return { status: 0, data: { error: err.message } }
    }
  }

  const publishEvent = (channel: string, event: string, data: string, tags?: Record<string, string>) => {
    const body: Record<string, unknown> = { name: event, channel, data }
    if (tags && Object.keys(tags).length > 0) body.tags = tags
    return apiRequest('POST', '/events', body)
  }

  const publishBatch = (events: Array<{ name: string; channel: string; data: string }>) =>
    apiRequest('POST', '/batch_events', { batch: events })

  const getChannels = (prefix?: string) => {
    const params: Record<string, string> = { info: 'subscription_count,user_count' }
    if (prefix) params.filter_by_prefix = prefix
    return apiRequest('GET', '/channels', undefined, params)
  }

  const getChannel = (name: string) =>
    apiRequest('GET', `/channels/${encodeURIComponent(name)}`, undefined, { info: 'subscription_count,user_count' })

  const getChannelUsers = (name: string) =>
    apiRequest('GET', `/channels/${encodeURIComponent(name)}/users`)

  const terminateUser = (userId: string) =>
    apiRequest('POST', `/users/${encodeURIComponent(userId)}/terminate_connections`)

  async function healthCheck() {
    try {
      const res = await fetch(`${baseUrl()}/up`)
      return { status: res.status, data: await res.text() }
    } catch (err: any) {
      return { status: 0, data: err.message }
    }
  }

  return { publishEvent, publishBatch, getChannels, getChannel, getChannelUsers, terminateUser, healthCheck }
}
