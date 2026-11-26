import {
  bindIdMapping,
  enqueueOutbox,
  generateEventId,
  generateSyncUid,
  getLocalIdBySyncUid,
  getOrCreateDeviceId,
  getSyncUidByLocalId,
  setLastServerSeq,
} from './metaDb'
import type { SnapshotResponse, SyncEntity, SyncEvent } from './types'
import { db } from '@/utils/db'
import type { IChapterRecord, IReviewRecord, IWordRecord } from '@/utils/db/record'
import type { Table } from 'dexie'

/**
 * Only sync learning progress and user preferences from localStorage,
 * excluding device-local UI flags such as promotion popups.
 */
export const SYNCED_STORAGE_KEYS = [
  'currentDict',
  'currentChapter',
  'unfinishedPractice',
  'reviewModeInfo',
  'loopWordConfig',
  'keySoundsConfig',
  'hintSoundsConfig',
  'pronunciation',
  'fontsize',
  'randomConfig',
  'phoneticConfig',
  'wordDictationConfig',
  'isShowPrevAndNextWord',
  'isIgnoreCase',
  'isShowAnswerOnHover',
  'isTextSelectable',
  'isOpenDarkModeAtom',
] as const

const SYNCED_KEY_SET = new Set<string>(SYNCED_STORAGE_KEYS)

let isApplyingRemote = false
let hooksRegistered = false

async function withRemoteGuard<T>(fn: () => Promise<T>): Promise<T> {
  isApplyingRemote = true
  try {
    return await fn()
  } finally {
    isApplyingRemote = false
  }
}

export interface HookCallbacks {
  getUserId: () => string
  onOutboxQueued: () => void
}

/**
 * Attach non-invasive Dexie hooks to RecordDB and localStorage observer
 * so existing business code keeps writing locally without any changes.
 */
export function registerLocalSyncHooks(cb: HookCallbacks): void {
  if (hooksRegistered) return
  hooksRegistered = true

  attachTableHooks('wordRecords', cb)
  attachTableHooks('chapterRecords', cb)
  attachTableHooks('reviewRecords', cb)
  attachLocalStorageHook(cb)
}

function attachTableHooks(entity: Exclude<SyncEntity, 'config'>, cb: HookCallbacks): void {
  const table = db[entity] as unknown as Table<Record<string, unknown>, number>

  table.hook('creating', function (_primKey, obj, trans) {
    if (isApplyingRemote) return
    const snapshot = clonePlain(obj)

    // Bind post-commit callback so rolled-back transactions never leak dirty events
    this.onsuccess = function (assignedKey) {
      const localId = Number(assignedKey)
      trans.on('complete', () => {
        void emitCreateEvent(entity, localId, snapshot, cb)
      })
    }
  })

  table.hook('updating', function (modifications, primKey, obj, trans) {
    if (isApplyingRemote) return
    const localId = Number(primKey)
    const merged = { ...clonePlain(obj), ...clonePlain(modifications) }
    trans.on('complete', () => {
      void emitUpdateEvent(entity, localId, merged, cb)
    })
  })

  table.hook('deleting', function (primKey, obj, trans) {
    if (isApplyingRemote) return
    const localId = Number(primKey)
    const snapshot = clonePlain(obj)
    trans.on('complete', () => {
      void emitDeleteEvent(entity, localId, snapshot, cb)
    })
  })
}

async function emitCreateEvent(
  entity: Exclude<SyncEntity, 'config'>,
  localId: number,
  rawPayload: Record<string, unknown>,
  cb: HookCallbacks,
): Promise<void> {
  const userId = cb.getUserId()
  if (!userId) return

  const deviceId = await getOrCreateDeviceId()
  let syncUid = await getSyncUidByLocalId(entity, localId)
  if (!syncUid) {
    syncUid = generateSyncUid(deviceId)
    await bindIdMapping(entity, localId, syncUid)
  }

  const payload = { ...rawPayload }
  delete payload.id

  // Map chapterRecords.wordRecordIds (local auto-increment IDs) to portable wordSyncUids
  if (entity === 'chapterRecords' && Array.isArray(payload.wordRecordIds)) {
    const uids: string[] = []
    for (const wid of payload.wordRecordIds) {
      const uid = await getSyncUidByLocalId('wordRecords', Number(wid))
      if (uid) uids.push(uid)
    }
    payload.wordSyncUids = uids
    delete payload.wordRecordIds
  }

  const event: SyncEvent = {
    eventId: generateEventId(deviceId),
    userId,
    sourceDeviceId: deviceId,
    entity,
    op: 'CREATE',
    syncUid,
    payload,
    clientTs: Date.now(),
  }
  await enqueueOutbox(event)
  cb.onOutboxQueued()
}

async function emitUpdateEvent(
  entity: Exclude<SyncEntity, 'config'>,
  localId: number,
  rawPayload: Record<string, unknown>,
  cb: HookCallbacks,
): Promise<void> {
  const userId = cb.getUserId()
  if (!userId) return

  const deviceId = await getOrCreateDeviceId()
  let syncUid = await getSyncUidByLocalId(entity, localId)
  if (!syncUid) {
    syncUid = generateSyncUid(deviceId)
    await bindIdMapping(entity, localId, syncUid)
  }

  const payload = { ...rawPayload }
  delete payload.id

  const event: SyncEvent = {
    eventId: generateEventId(deviceId),
    userId,
    sourceDeviceId: deviceId,
    entity,
    op: 'UPDATE',
    syncUid,
    payload,
    clientTs: Date.now(),
  }
  await enqueueOutbox(event)
  cb.onOutboxQueued()
}

async function emitDeleteEvent(
  entity: Exclude<SyncEntity, 'config'>,
  localId: number,
  rawPayload: Record<string, unknown>,
  cb: HookCallbacks,
): Promise<void> {
  const userId = cb.getUserId()
  if (!userId) return

  const deviceId = await getOrCreateDeviceId()
  const syncUid = await getSyncUidByLocalId(entity, localId)
  if (!syncUid) return

  const event: SyncEvent = {
    eventId: generateEventId(deviceId),
    userId,
    sourceDeviceId: deviceId,
    entity,
    op: 'DELETE',
    syncUid,
    payload: { word: rawPayload.word, dict: rawPayload.dict },
    clientTs: Date.now(),
  }
  await enqueueOutbox(event)
  cb.onOutboxQueued()
}

function attachLocalStorageHook(cb: HookCallbacks): void {
  if (typeof window === 'undefined' || !window.localStorage) return
  const proto = Storage.prototype
  const originalSetItem = proto.setItem

  proto.setItem = function patchedSetItem(key: string, value: string): void {
    const prev = this === window.localStorage ? this.getItem(key) : null
    originalSetItem.call(this, key, value)

    if (this !== window.localStorage || isApplyingRemote || !SYNCED_KEY_SET.has(key) || prev === value) {
      return
    }
    void emitConfigUpdate(key, value, cb)
  }
}

async function emitConfigUpdate(key: string, value: string, cb: HookCallbacks): Promise<void> {
  const userId = cb.getUserId()
  if (!userId) return

  const deviceId = await getOrCreateDeviceId()
  const event: SyncEvent = {
    eventId: generateEventId(deviceId),
    userId,
    sourceDeviceId: deviceId,
    entity: 'config',
    op: 'UPDATE',
    syncUid: key,
    payload: { key, value },
    clientTs: Date.now(),
  }
  await enqueueOutbox(event)
  cb.onOutboxQueued()
}

/**
 * Apply a single downstream SyncEvent from another device into local storage
 * without triggering local CDC hooks.
 */
export async function applyRemoteEvent(ev: SyncEvent): Promise<void> {
  const selfDeviceId = await getOrCreateDeviceId()
  if (ev.sourceDeviceId === selfDeviceId) {
    if (ev.serverSeq) await setLastServerSeq(ev.serverSeq)
    return
  }

  await withRemoteGuard(async () => {
    if (ev.entity === 'config') {
      applyRemoteConfig(ev.syncUid, ev.payload)
      return
    }

    if (ev.op === 'DELETE_BY_WORD' && ev.entity === 'wordRecords') {
      const word = String(ev.payload.word ?? '')
      const dict = String(ev.payload.dict ?? '')
      if (word && dict) {
        await db.wordRecords.where({ word, dict }).delete()
      }
      return
    }

    if (ev.op === 'DELETE') {
      const localId = await getLocalIdBySyncUid(ev.entity, ev.syncUid)
      if (localId !== undefined) {
        await db[ev.entity].delete(localId)
      }
      return
    }

    await upsertLocalRecord(ev.entity, ev.syncUid, ev.payload)
  })

  if (ev.serverSeq) {
    await setLastServerSeq(ev.serverSeq)
  }
}

/**
 * Apply a cold-start or catch-up snapshot returned by sync.req.snapshot.{userId}.
 */
export async function applySnapshot(snap: SnapshotResponse): Promise<void> {
  await withRemoteGuard(async () => {
    // Apply wordRecords first so chapterRecords can resolve wordSyncUids -> localId
    const ordered = [...snap.records].sort((a, b) => entityOrder(a.entity) - entityOrder(b.entity))
    for (const rec of ordered) {
      if (rec.entity === 'config') {
        applyRemoteConfig(rec.syncUid, rec.payload)
      } else {
        await upsertLocalRecord(rec.entity, rec.syncUid, rec.payload)
      }
    }
  })
  if (snap.latestServerSeq > 0) {
    await setLastServerSeq(snap.latestServerSeq)
  }
}

function entityOrder(entity: SyncEntity): number {
  switch (entity) {
    case 'wordRecords':
      return 0
    case 'chapterRecords':
      return 1
    case 'reviewRecords':
      return 2
    case 'config':
      return 3
  }
}

async function upsertLocalRecord(
  entity: Exclude<SyncEntity, 'config'>,
  syncUid: string,
  rawPayload: Record<string, unknown>,
): Promise<void> {
  const existingLocalId = await getLocalIdBySyncUid(entity, syncUid)

  if (entity === 'wordRecords') {
    const data = rawPayload as unknown as IWordRecord
    if (existingLocalId !== undefined) {
      await db.wordRecords.update(existingLocalId, data)
    } else {
      const newId = await db.wordRecords.add(data)
      await bindIdMapping('wordRecords', newId, syncUid)
    }
    return
  }

  if (entity === 'chapterRecords') {
    const payload = { ...rawPayload }
    const wordRecordIds: number[] = []
    if (Array.isArray(payload.wordSyncUids)) {
      for (const uid of payload.wordSyncUids) {
        const lid = await getLocalIdBySyncUid('wordRecords', String(uid))
        if (lid !== undefined) wordRecordIds.push(lid)
      }
    }
    delete payload.wordSyncUids
    const data = { ...payload, wordRecordIds } as unknown as IChapterRecord
    if (existingLocalId !== undefined) {
      await db.chapterRecords.update(existingLocalId, data)
    } else {
      const newId = await db.chapterRecords.add(data)
      await bindIdMapping('chapterRecords', newId, syncUid)
    }
    return
  }

  if (entity === 'reviewRecords') {
    const incoming = rawPayload as unknown as IReviewRecord
    if (existingLocalId !== undefined) {
      const current = await db.reviewRecords.get(existingLocalId)
      const merged: IReviewRecord = {
        ...incoming,
        id: existingLocalId,
        index: Math.max(current?.index ?? 0, incoming.index ?? 0),
        isFinished: Boolean(current?.isFinished || incoming.isFinished),
      }
      await db.reviewRecords.put(merged)
    } else {
      const newId = await db.reviewRecords.add({ ...incoming, id: undefined })
      await bindIdMapping('reviewRecords', newId, syncUid)
    }
  }
}

function applyRemoteConfig(key: string, payload: Record<string, unknown>): void {
  if (!SYNCED_KEY_SET.has(key) || typeof window === 'undefined') return
  const newValue = typeof payload.value === 'string' ? payload.value : JSON.stringify(payload.value)
  const oldValue = window.localStorage.getItem(key)
  if (oldValue === newValue) return

  window.localStorage.setItem(key, newValue)
  // Notify same-tab Jotai atomWithStorage listeners without changing any store code
  window.dispatchEvent(
    new StorageEvent('storage', {
      key,
      oldValue,
      newValue,
      storageArea: window.localStorage,
    }),
  )
}

function clonePlain(obj: unknown): Record<string, unknown> {
  return JSON.parse(JSON.stringify(obj ?? {}))
}
