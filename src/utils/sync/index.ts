import type { BackfillResult } from './backfill'
import { hasCompletedInitialBackfill, scanAndEnqueueHistoricalData } from './backfill'
import { applyRemoteEvent, applySnapshot, registerLocalSyncHooks } from './hooks'
import { getLastServerSeq, getOrCreateDeviceId, listOutbox, removeOutboxByEventId, resetLastServerSeq, setLastServerSeq } from './metaDb'
import { NatsWsClient } from './natsWsClient'
import type { SnapshotRequest, SnapshotResponse, SyncConfig, SyncEvent } from './types'

const CONFIG_STORAGE_KEY = 'qwerty_sync_config'

const decoder = new TextDecoder()

export interface SyncStatusSnapshot {
  enabled: boolean
  connected: boolean
  pendingOutbox: number
  lastServerSeq: number
  deviceId: string
}

type StatusListener = (status: SyncStatusSnapshot) => void

let client: NatsWsClient | null = null
let activeConfig: SyncConfig | null = null
let isFlushing = false
let flushRequested = false
let inbound = Promise.resolve()

function serializeInbound(work: () => Promise<void>): Promise<void> {
  const result = inbound.then(work)
  inbound = result.catch((err) => console.error('[sync] failed to apply inbound changes:', err))
  return result
}

let unsubDown: (() => void) | null = null
const statusListeners = new Set<StatusListener>()

export function loadSyncConfig(): SyncConfig {
  try {
    const raw = window.localStorage.getItem(CONFIG_STORAGE_KEY)
    if (raw) {
      const parsed = JSON.parse(raw) as Partial<SyncConfig>
      if (parsed.wsUrl || parsed.userId || parsed.token) {
        return {
          enabled: parsed.enabled !== false,
          wsUrl: /^ws:\/\/(127\.0\.0\.1|localhost|\[::1\]):9222\/?$/.test(parsed.wsUrl ?? '') ? 'ws://127.0.0.1:9322' : parsed.wsUrl ?? '',
          token: parsed.token ?? '',
          userId: parsed.userId ?? '',
        }
      }
    }
  } catch {
    // ignore malformed local config
  }

  const envUrl = import.meta.env.VITE_SYNC_WS_URL as string | undefined
  const envToken = import.meta.env.VITE_SYNC_TOKEN as string | undefined
  const envUserId = import.meta.env.VITE_SYNC_USER_ID as string | undefined

  // Default to local auto-started sync-server unless explicitly disabled in settings
  return {
    enabled: true,
    wsUrl: envUrl || 'ws://127.0.0.1:9322',
    token: envToken || 'qwerty-dev-token',
    userId: envUserId || 'local-user',
  }
}

export function saveSyncConfig(cfg: SyncConfig): void {
  window.localStorage.setItem(CONFIG_STORAGE_KEY, JSON.stringify(cfg))
}

export async function getSyncStatus(): Promise<SyncStatusSnapshot> {
  const cfg = activeConfig ?? loadSyncConfig()
  const [deviceId, lastServerSeq, pending] = await Promise.all([getOrCreateDeviceId(), getLastServerSeq(cfg), listOutbox()])
  return {
    enabled: Boolean(cfg.enabled && cfg.wsUrl && cfg.userId),
    connected: client?.isConnected() ?? false,
    pendingOutbox: pending.length,
    lastServerSeq,
    deviceId,
  }
}

function notifyStatusListeners(): void {
  if (statusListeners.size === 0) return
  void getSyncStatus().then((snap) => {
    statusListeners.forEach((fn) => fn(snap))
  })
}

export function subscribeSyncStatus(listener: StatusListener): () => void {
  statusListeners.add(listener)
  void getSyncStatus().then((snap) => listener(snap))
  return () => {
    statusListeners.delete(listener)
  }
}

async function flushOutbox(): Promise<void> {
  if (isFlushing) {
    flushRequested = true
    return
  }
  if (!client?.isConnected() || !activeConfig) return
  flushRequested = false
  isFlushing = true
  try {
    const connection = client
    const pending = await listOutbox()
    for (const row of pending) {
      const subject = `sync.up.${row.event.userId}`
      if (client !== connection) break
      try {
        const ack = JSON.parse(decoder.decode(await connection.request(subject, JSON.stringify(row.event))))
        if (ack.stream !== 'SYNC_UP' || !ack.seq || ack.error) throw new Error('JetStream did not acknowledge the event')
      } catch {
        if (client === connection && connection.isConnected()) window.setTimeout(() => void flushOutbox(), 2000)
        break
      }
      await removeOutboxByEventId(row.eventId)
    }
  } finally {
    isFlushing = false
    notifyStatusListeners()
    if (flushRequested) void flushOutbox()
  }
}

async function pullCatchUpSnapshot(): Promise<void> {
  if (!client || !client.isConnected() || !activeConfig) return
  const cfg = activeConfig
  const connection = client
  const deviceId = await getOrCreateDeviceId()
  // Replay once to recover deletions omitted by older snapshot responses.
  const migrationKey = `qwerty_sync_tombstones:${cfg.wsUrl}:${cfg.userId}`
  const afterSeq = localStorage.getItem(migrationKey) ? await getLastServerSeq(cfg) : 0
  if (client !== connection) return
  const req: SnapshotRequest = {
    userId: cfg.userId,
    deviceId,
    afterSeq,
  }
  try {
    const records = new Map<string, SnapshotResponse['records'][number]>()
    const subject = `sync.req.snapshot.${req.userId}`
    let latestServerSeq: number
    for (;;) {
      const raw = await connection.request(subject, JSON.stringify(req))
      if (client !== connection) return
      const snap = JSON.parse(decoder.decode(raw)) as SnapshotResponse
      if ((req.afterSeq ?? 0) > snap.latestServerSeq) {
        // The server database was recreated; replay its new sequence from zero.
        await resetLastServerSeq(cfg)
        records.clear()
        req.afterSeq = 0
        delete req.afterRow
        continue
      }
      for (const record of snap.records) records.set(JSON.stringify([record.entity, record.syncUid]), record)
      latestServerSeq = snap.latestServerSeq
      if (!snap.nextAfterSeq) break
      if (
        snap.nextAfterSeq < (req.afterSeq ?? 0) ||
        (snap.nextAfterSeq === req.afterSeq && (snap.nextAfterRow ?? 0) <= (req.afterRow ?? 0))
      )
        throw new Error('Invalid snapshot cursor')
      req.afterSeq = snap.nextAfterSeq
      req.afterRow = snap.nextAfterRow
    }
    await applySnapshot({ userId: req.userId, latestServerSeq, records: Array.from(records.values()) })
    await setLastServerSeq(cfg, latestServerSeq)
    localStorage.setItem(migrationKey, '1')
  } catch (err) {
    console.warn('[sync] catch-up snapshot skipped:', err)
  } finally {
    notifyStatusListeners()
  }
}

function startClient(cfg: SyncConfig): void {
  stopClient()
  activeConfig = cfg
  if (!cfg.enabled || !cfg.wsUrl || !cfg.userId) {
    notifyStatusListeners()
    return
  }

  client = new NatsWsClient({
    url: cfg.wsUrl,
    token: cfg.token,
    name: `qwerty-${cfg.userId}`,
    onConnected: () => {
      notifyStatusListeners()
      void (async () => {
        await serializeInbound(async () => {
          if (activeConfig !== cfg) return
          if (!(await hasCompletedInitialBackfill(cfg.userId))) {
            await scanAndEnqueueHistoricalData(cfg.userId)
            notifyStatusListeners()
          }
          await pullCatchUpSnapshot()
        })
        await flushOutbox()
      })().catch((err) => console.error('[sync] startup sync failed:', err))
    },
    onDisconnected: () => {
      notifyStatusListeners()
    },
  })

  unsubDown = client.subscribe(`sync.down.${cfg.userId}`, (_subject, payload) => {
    try {
      const ev = JSON.parse(decoder.decode(payload)) as SyncEvent
      void serializeInbound(async () => {
        if (activeConfig === cfg && (!ev.serverSeq || ev.serverSeq > (await getLastServerSeq(cfg)))) {
          await applyRemoteEvent(ev)
          if (ev.serverSeq) await setLastServerSeq(cfg, ev.serverSeq)
        }
        notifyStatusListeners()
      }).catch(() => undefined)
    } catch (err) {
      console.error('[sync] failed to handle downstream event:', err)
    }
  })

  client.start()
  notifyStatusListeners()
}

function stopClient(): void {
  if (unsubDown) {
    unsubDown()
    unsubDown = null
  }
  if (client) {
    client.close()
    client = null
  }
}

export function applySyncConfig(cfg: SyncConfig): void {
  saveSyncConfig(cfg)
  if (cfg.enabled && cfg.wsUrl && cfg.userId) {
    startClient(cfg)
  } else {
    activeConfig = cfg
    stopClient()
    notifyStatusListeners()
  }
}

export async function triggerManualSync(): Promise<void> {
  await serializeInbound(pullCatchUpSnapshot)
  await flushOutbox()
}

export async function syncHistoricalData(): Promise<BackfillResult> {
  const cfg = activeConfig ?? loadSyncConfig()
  if (!cfg.userId) {
    return { scanned: 0, queued: 0 }
  }
  const result = await scanAndEnqueueHistoricalData(cfg.userId)
  notifyStatusListeners()
  await flushOutbox()
  return result
}

/**
 * Initialize the MQ sync engine once at app boot.
 * Stays completely idle unless configured via env or localStorage.
 */
export function initSyncEngine(): void {
  const cfg = loadSyncConfig()
  activeConfig = cfg

  registerLocalSyncHooks({
    getUserId: () => (activeConfig?.enabled ? activeConfig.userId : ''),
    onOutboxQueued: () => {
      notifyStatusListeners()
      void flushOutbox()
    },
  })

  if (cfg.enabled && cfg.wsUrl && cfg.userId) {
    startClient(cfg)
  }

  // Expose a convenient console helper for local Web + Tauri testing
  const win = window as unknown as Record<string, unknown>
  win.__qwertySync = {
    enable(opts: { wsUrl?: string; token?: string; userId?: string }) {
      const next: SyncConfig = {
        enabled: true,
        wsUrl: opts.wsUrl || 'ws://127.0.0.1:9322',
        token: opts.token || 'qwerty-dev-token',
        userId: opts.userId || 'local-user',
      }
      applySyncConfig(next)
      return next
    },
    disable() {
      const next: SyncConfig = { ...(activeConfig ?? loadSyncConfig()), enabled: false }
      applySyncConfig(next)
    },
    flush: () => triggerManualSync(),
    syncHistory: () => syncHistoricalData(),
    getStatus: () => getSyncStatus(),
  }
}
