import { SYNCED_STORAGE_KEYS } from './hooks'
import { bindIdMapping, enqueueOutbox, getOrCreateDeviceId, getSyncUidByLocalId, syncMetaDb } from './metaDb'
import type { SyncEvent } from './types'
import { db } from '@/utils/db'
import type { IChapterRecord, IReviewRecord, IWordRecord } from '@/utils/db/record'

export interface BackfillResult {
  scanned: number
  queued: number
}

/**
 * Deterministic 64-bit-like hex hash (dual 32-bit FNV-1a) that works in both
 * HTTPS and plain HTTP / Tauri WebView contexts without SubtleCrypto.
 */
function deterministicHash(input: string): string {
  let h1 = 0x811c9dc5
  let h2 = 0x01000193
  for (let i = 0; i < input.length; i++) {
    const ch = input.charCodeAt(i)
    h1 ^= ch
    h1 = Math.imul(h1, 0x01000193)
    h2 ^= ch + i
    h2 = Math.imul(h2, 0x85ebca6b)
  }
  return (h1 >>> 0).toString(16).padStart(8, '0') + (h2 >>> 0).toString(16).padStart(8, '0')
}

function fingerprintWord(rec: IWordRecord): string {
  const key = `w|${rec.dict}|${rec.chapter ?? 'null'}|${rec.word}|${rec.timeStamp}|${rec.wrongCount}|${(rec.timing ?? []).join(',')}`
  return `fp_w_${deterministicHash(key)}`
}

function fingerprintChapter(rec: IChapterRecord): string {
  const key = `c|${rec.dict}|${rec.chapter ?? 'null'}|${rec.timeStamp}|${rec.time}|${rec.wordCount}|${rec.correctCount}|${rec.wrongCount}`
  return `fp_c_${deterministicHash(key)}`
}

function fingerprintReview(rec: IReviewRecord): string {
  const key = `r|${rec.dict}|${rec.createTime}`
  return `fp_r_${deterministicHash(key)}`
}

export async function hasCompletedInitialBackfill(userId: string): Promise<boolean> {
  const row = await syncMetaDb.kv.get(`backfilled:${userId}`)
  return Boolean(row && row.value === '1')
}

export async function markInitialBackfillDone(userId: string): Promise<void> {
  await syncMetaDb.kv.put({ key: `backfilled:${userId}`, value: '1' })
}

/**
 * Scans existing local RecordDB tables and whitelisted localStorage keys,
 * binds deterministic content-fingerprint syncUids for unmapped records,
 * and enqueues idempotent backfill events into SyncMetaDB.outbox.
 */
export async function scanAndEnqueueHistoricalData(userId: string): Promise<BackfillResult> {
  if (!userId) return { scanned: 0, queued: 0 }

  const deviceId = await getOrCreateDeviceId()
  let scanned = 0
  let queued = 0

  // 1. wordRecords first so chapterRecords can resolve wordRecordIds -> wordSyncUids
  // Read keys + rows from RecordDB before touching SyncMetaDB to avoid IndexedDB TransactionInactiveError.
  const [wordKeys, wordRows] = await Promise.all([db.wordRecords.toCollection().primaryKeys(), db.wordRecords.toArray()])
  for (let i = 0; i < wordRows.length; i++) {
    const record = wordRows[i]
    const localId = Number(wordKeys[i])
    scanned += 1
    const existingUid = await getSyncUidByLocalId('wordRecords', localId)
    if (existingUid) continue

    const syncUid = fingerprintWord(record)
    await bindIdMapping('wordRecords', localId, syncUid)

    const payload = JSON.parse(JSON.stringify(record)) as Record<string, unknown>
    delete payload.id

    const event: SyncEvent = {
      eventId: `backfill:${userId}:wordRecords:${syncUid}`,
      userId,
      sourceDeviceId: deviceId,
      entity: 'wordRecords',
      op: 'CREATE',
      syncUid,
      payload,
      clientTs: record.timeStamp ? record.timeStamp * 1000 : Date.now(),
    }
    await enqueueOutbox(event)
    queued += 1
  }

  // 2. chapterRecords
  const [chapterKeys, chapterRows] = await Promise.all([
    db.chapterRecords.toCollection().primaryKeys(),
    db.chapterRecords.toArray(),
  ])
  for (let i = 0; i < chapterRows.length; i++) {
    const record = chapterRows[i]
    const localId = Number(chapterKeys[i])
    scanned += 1
    const existingUid = await getSyncUidByLocalId('chapterRecords', localId)
    if (existingUid) continue

    const syncUid = fingerprintChapter(record)
    await bindIdMapping('chapterRecords', localId, syncUid)

    const wordSyncUids: string[] = []
    for (const wid of record.wordRecordIds ?? []) {
      const uid = await getSyncUidByLocalId('wordRecords', Number(wid))
      if (uid) wordSyncUids.push(uid)
    }

    const payload = JSON.parse(JSON.stringify(record)) as Record<string, unknown>
    delete payload.id
    delete payload.wordRecordIds
    payload.wordSyncUids = wordSyncUids

    const event: SyncEvent = {
      eventId: `backfill:${userId}:chapterRecords:${syncUid}`,
      userId,
      sourceDeviceId: deviceId,
      entity: 'chapterRecords',
      op: 'CREATE',
      syncUid,
      payload,
      clientTs: record.timeStamp ? record.timeStamp * 1000 : Date.now(),
    }
    await enqueueOutbox(event)
    queued += 1
  }

  // 3. reviewRecords
  const [reviewKeys, reviewRows] = await Promise.all([
    db.reviewRecords.toCollection().primaryKeys(),
    db.reviewRecords.toArray(),
  ])
  for (let i = 0; i < reviewRows.length; i++) {
    const record = reviewRows[i]
    const localId = Number(reviewKeys[i])
    scanned += 1
    const existingUid = await getSyncUidByLocalId('reviewRecords', localId)
    if (existingUid) continue

    const syncUid = fingerprintReview(record)
    await bindIdMapping('reviewRecords', localId, syncUid)

    const payload = JSON.parse(JSON.stringify(record)) as Record<string, unknown>
    delete payload.id

    const event: SyncEvent = {
      eventId: `backfill:${userId}:reviewRecords:${syncUid}`,
      userId,
      sourceDeviceId: deviceId,
      entity: 'reviewRecords',
      op: 'UPDATE',
      syncUid,
      payload,
      clientTs: record.createTime ? record.createTime * 1000 : Date.now(),
    }
    await enqueueOutbox(event)
    queued += 1
  }

  // 4. Whitelisted localStorage preferences (only if not yet backfilled for this user)
  const alreadyBackfilled = await hasCompletedInitialBackfill(userId)
  if (!alreadyBackfilled && typeof window !== 'undefined' && window.localStorage) {
    const now = Date.now()
    for (const key of SYNCED_STORAGE_KEYS) {
      const value = window.localStorage.getItem(key)
      if (value === null) continue
      scanned += 1
      const event: SyncEvent = {
        eventId: `backfill:${userId}:config:${key}:${deterministicHash(value)}`,
        userId,
        sourceDeviceId: deviceId,
        entity: 'config',
        op: 'UPDATE',
        syncUid: key,
        payload: { key, value },
        clientTs: now,
      }
      await enqueueOutbox(event)
      queued += 1
    }
  }

  await markInitialBackfillDone(userId)
  return { scanned, queued }
}
