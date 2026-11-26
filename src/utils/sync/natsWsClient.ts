/**
 * Zero-dependency NATS-over-WebSocket client implementing the text wire protocol
 * (INFO / CONNECT / PING / PONG / PUB / SUB / UNSUB / MSG) for both Web and Tauri WebView.
 */

export type NatsMessageHandler = (subject: string, payload: Uint8Array, replyTo?: string) => void

export interface NatsWsOptions {
  url: string
  token?: string
  name?: string
  onConnected?: () => void
  onDisconnected?: () => void
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

export class NatsWsClient {
  private ws: WebSocket | null = null
  private connected = false
  private closed = false
  private reconnectTimer: number | null = null
  private reconnectDelayMs = 1000
  private nextSid = 1
  private buffer = new Uint8Array(0)
  private subs = new Map<string, { subject: string; handler: NatsMessageHandler }>()

  constructor(private readonly opts: NatsWsOptions) {}

  public isConnected(): boolean {
    return this.connected
  }

  public start(): void {
    this.closed = false
    this.connect()
  }

  public close(): void {
    this.closed = true
    this.connected = false
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    if (this.ws) {
      this.ws.close()
      this.ws = null
    }
  }

  public publish(subject: string, data: string | Uint8Array, replyTo?: string): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN || !this.connected) {
      return false
    }
    const payload = typeof data === 'string' ? encoder.encode(data) : data
    const header = replyTo ? `PUB ${subject} ${replyTo} ${payload.byteLength}\r\n` : `PUB ${subject} ${payload.byteLength}\r\n`
    this.sendRaw(concatBytes(encoder.encode(header), payload, encoder.encode('\r\n')))
    return true
  }

  public subscribe(subject: string, handler: NatsMessageHandler): () => void {
    const sid = String(this.nextSid++)
    this.subs.set(sid, { subject, handler })
    if (this.connected) {
      this.sendLine(`SUB ${subject} ${sid}`)
    }
    return () => {
      this.subs.delete(sid)
      if (this.connected) {
        this.sendLine(`UNSUB ${sid}`)
      }
    }
  }

  public request(subject: string, data: string | Uint8Array, timeoutMs = 8000): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      if (!this.connected) {
        reject(new Error('NATS WebSocket is not connected'))
        return
      }
      const inbox = `_INBOX.qwerty.${Date.now()}.${Math.random().toString(36).slice(2, 10)}`
      let timer: number | null = null
      const unsub = this.subscribe(inbox, (_subj, payload) => {
        if (timer !== null) window.clearTimeout(timer)
        unsub()
        resolve(payload)
      })
      timer = window.setTimeout(() => {
        unsub()
        reject(new Error(`NATS request timeout for ${subject}`))
      }, timeoutMs)

      if (!this.publish(subject, data, inbox)) {
        if (timer !== null) window.clearTimeout(timer)
        unsub()
        reject(new Error('Failed to send NATS request'))
      }
    })
  }

  private connect(): void {
    if (this.closed) return
    try {
      const ws = new WebSocket(this.opts.url)
      ws.binaryType = 'arraybuffer'
      this.ws = ws
      this.buffer = new Uint8Array(0)

      ws.onmessage = (evt) => {
        if (evt.data instanceof ArrayBuffer) {
          this.onBytes(new Uint8Array(evt.data))
        } else if (typeof evt.data === 'string') {
          this.onBytes(encoder.encode(evt.data))
        }
      }

      ws.onclose = () => {
        const wasConnected = this.connected
        this.connected = false
        this.ws = null
        if (wasConnected) {
          this.opts.onDisconnected?.()
        }
        this.scheduleReconnect()
      }

      ws.onerror = () => {
        // onclose will handle reconnect
      }
    } catch {
      this.scheduleReconnect()
    }
  }

  private scheduleReconnect(): void {
    if (this.closed || this.reconnectTimer !== null) return
    const delay = this.reconnectDelayMs
    this.reconnectDelayMs = Math.min(this.reconnectDelayMs * 2, 15000)
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }

  private sendLine(line: string): void {
    this.sendRaw(encoder.encode(line + '\r\n'))
  }

  private sendRaw(bytes: Uint8Array): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(bytes)
    }
  }

  private onBytes(chunk: Uint8Array): void {
    this.buffer = concatBytes(this.buffer, chunk)

    while (true) {
      const crlfIdx = findCRLF(this.buffer)
      if (crlfIdx === -1) return

      const line = decoder.decode(this.buffer.subarray(0, crlfIdx))

      if (line.startsWith('MSG ')) {
        const parts = line.split(' ')
        // MSG <subject> <sid> [reply-to] <#bytes>
        const subject = parts[1]
        const sid = parts[2]
        const replyTo = parts.length === 5 ? parts[3] : undefined
        const byteLen = parseInt(parts[parts.length - 1], 10)
        const totalNeeded = crlfIdx + 2 + byteLen + 2
        if (this.buffer.byteLength < totalNeeded) {
          return // Wait for complete payload
        }
        const payload = this.buffer.slice(crlfIdx + 2, crlfIdx + 2 + byteLen)
        this.buffer = this.buffer.slice(totalNeeded)

        const sub = this.subs.get(sid)
        if (sub) {
          sub.handler(subject, payload, replyTo)
        }
        continue
      }

      this.buffer = this.buffer.slice(crlfIdx + 2)

      if (line.startsWith('INFO ')) {
        const connectPayload: Record<string, unknown> = {
          verbose: false,
          pedantic: false,
          lang: 'ts',
          version: '0.1.0',
          protocol: 1,
          name: this.opts.name || 'qwerty-learner-client',
        }
        if (this.opts.token) {
          connectPayload.auth_token = this.opts.token
        }
        this.sendLine(`CONNECT ${JSON.stringify(connectPayload)}`)
        this.sendLine('PING')
        this.connected = true
        this.reconnectDelayMs = 1000
        // Resubscribe active subjects after reconnect
        this.subs.forEach((sub, sid) => {
          this.sendLine(`SUB ${sub.subject} ${sid}`)
        })
        this.opts.onConnected?.()
      } else if (line === 'PING') {
        this.sendLine('PONG')
      }
    }
  }
}

function findCRLF(buf: Uint8Array): number {
  for (let i = 0; i < buf.byteLength - 1; i++) {
    if (buf[i] === 13 && buf[i + 1] === 10) {
      return i
    }
  }
  return -1
}

function concatBytes(...arrays: Uint8Array[]): Uint8Array {
  let total = 0
  for (const a of arrays) total += a.byteLength
  const out = new Uint8Array(total)
  let offset = 0
  for (const a of arrays) {
    out.set(a, offset)
    offset += a.byteLength
  }
  return out
}
