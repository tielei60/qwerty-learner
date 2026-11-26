import type { BackfillResult } from './backfill'
import { hasCompletedInitialBackfill, scanAndEnqueueHistoricalData } from './backfill'
import { applyRemoteEvent, applySnapshot, registerLocalSyncHooks } from './hooks'
import { getLastServerSeq, getOrCreateDeviceId, listOutbox, removeOutboxByEventId } from './metaDb'
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
          wsUrl: /^ws:\/\/(127\.0\.0\.1|localhost|\[::1\]):9222\/?$/.test(parsed.wsUrl ?? '')
            ? 'ws://127.0.0.1:9322'
            : parsed.wsUrl ?? '',
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
  const [deviceId, lastServerSeq, pending] = await Promise.all([getOrCreateDeviceId(), getLastServerSeq(), listOutbox()])
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
    const pending = await listOutbox()
    for (const row of pending) {
      const subject = `sync.up.${row.event.userId}`
      const ok = client.publish(subject, JSON.stringify(row.event))
      if (!ok) break
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
  const deviceId = await getOrCreateDeviceId()
  // Older builds can advance the shared cursor without knowing this preference.
  const afterSeq = localStorage.getItem('unfinishedPractice') === null ? 0 : await getLastServerSeq()
  const req: SnapshotRequest = {
    userId: activeConfig.userId,
    deviceId,
    afterSeq,
  }
  try {
    const raw = await client.request(`sync.req.snapshot.${activeConfig.userId}`, JSON.stringify(req))
    const snap = JSON.parse(decoder.decode(raw)) as SnapshotResponse
    if (snap && Array.isArray(snap.records) && snap.records.length > 0) {
      await applySnapshot(snap)
    }
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
        await pullCatchUpSnapshot()
        if (activeConfig && !(await hasCompletedInitialBackfill(activeConfig.userId))) {
          await scanAndEnqueueHistoricalData(activeConfig.userId)
          notifyStatusListeners()
        }
        await flushOutbox()
      })()
    },
    onDisconnected: () => {
      notifyStatusListeners()
    },
  })

  unsubDown = client.subscribe(`sync.down.${cfg.userId}`, (_subject, payload) => {
    try {
      const ev = JSON.parse(decoder.decode(payload)) as SyncEvent
      void applyRemoteEvent(ev).finally(() => notifyStatusListeners())
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
  await pullCatchUpSnapshot()
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
