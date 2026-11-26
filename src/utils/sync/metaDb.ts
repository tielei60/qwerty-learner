import type { SyncEntity, SyncEvent } from './types'
import type { Table } from 'dexie'
import Dexie from 'dexie'

export interface OutboxRow {
  id?: number
  eventId: string
  event: SyncEvent
  createdAt: number
}

export interface IdMapRow {
  entity: SyncEntity
  localId: number
  syncUid: string
}

export interface SyncKVRow {
  key: string
  value: string | number
}

/**
 * Separate IndexedDB for sync metadata so RecordDB's schema and
 * dexie-export-import compatibility remain completely untouched.
 */
class SyncMetaDB extends Dexie {
  outbox!: Table<OutboxRow, number>
  idMap!: Table<IdMapRow, [SyncEntity, number]>
  kv!: Table<SyncKVRow, string>

  constructor() {
    super('QwertySyncMetaDB')
    this.version(1).stores({
      outbox: '++id, &eventId, createdAt',
      idMap: '&[entity+localId], &[entity+syncUid]',
      kv: '&key',
    })
  }
}

export const syncMetaDb = new SyncMetaDB()

let seqCounter = 0

function randomHex(len: number): string {
  const bytes = new Uint8Array(Math.ceil(len / 2))
  if (typeof crypto !== 'undefined' && crypto.getRandomValues) {
    crypto.getRandomValues(bytes)
  } else {
    for (let i = 0; i < bytes.length; i++) {
      bytes[i] = Math.floor(Math.random() * 256)
    }
  }
  return Array.from(bytes, (b) => b.toString(16).padStart(2, '0'))
    .join('')
    .slice(0, len)
}

export function generateEventId(deviceId: string): string {
  seqCounter += 1
  return `${deviceId}:${Date.now()}:${seqCounter}:${randomHex(6)}`
}

export function generateSyncUid(deviceId: string): string {
  seqCounter += 1
  return `${deviceId}:${Date.now()}:${seqCounter}`
}

let cachedDeviceId: string | null = null

export async function getOrCreateDeviceId(): Promise<string> {
  if (cachedDeviceId) return cachedDeviceId
  const row = await syncMetaDb.kv.get('deviceId')
  if (row && typeof row.value === 'string' && row.value) {
    cachedDeviceId = row.value
    return cachedDeviceId
  }
  const isTauri = typeof window !== 'undefined' && '__TAURI__' in window
  const prefix = isTauri ? 'tauri' : 'web'
  const id = `${prefix}_${randomHex(10)}`
  await syncMetaDb.kv.put({ key: 'deviceId', value: id })
  cachedDeviceId = id
  return id
}

export async function getLastServerSeq(): Promise<number> {
  const row = await syncMetaDb.kv.get('lastServerSeq')
  return row && typeof row.value === 'number' ? row.value : 0
}

export async function setLastServerSeq(seq: number): Promise<void> {
  const current = await getLastServerSeq()
  if (seq > current) {
    await syncMetaDb.kv.put({ key: 'lastServerSeq', value: seq })
  }
}

export async function bindIdMapping(entity: SyncEntity, localId: number, syncUid: string): Promise<void> {
  await syncMetaDb.idMap.put({ entity, localId, syncUid })
}

export async function getSyncUidByLocalId(entity: SyncEntity, localId: number): Promise<string | undefined> {
  const row = await syncMetaDb.idMap.get([entity, localId])
  return row?.syncUid
}

export async function getLocalIdBySyncUid(entity: SyncEntity, syncUid: string): Promise<number | undefined> {
  const row = await syncMetaDb.idMap.where('[entity+syncUid]').equals([entity, syncUid]).first()
  return row?.localId
}

export async function enqueueOutbox(event: SyncEvent): Promise<void> {
  await syncMetaDb.outbox.put({
    eventId: event.eventId,
    event,
    createdAt: Date.now(),
  })
}

export async function listOutbox(): Promise<OutboxRow[]> {
  return syncMetaDb.outbox.orderBy('id').toArray()
}

export async function removeOutboxByEventId(eventId: string): Promise<void> {
  await syncMetaDb.outbox.where('eventId').equals(eventId).delete()
}
