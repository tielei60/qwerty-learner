export type SyncEntity = 'wordRecords' | 'chapterRecords' | 'reviewRecords' | 'config'

export type SyncOp = 'CREATE' | 'UPDATE' | 'DELETE' | 'DELETE_BY_WORD'

export interface SyncEvent<T = Record<string, unknown>> {
  eventId: string
  userId: string
  sourceDeviceId: string
  entity: SyncEntity
  op: SyncOp
  syncUid: string
  payload: T
  clientTs: number
  serverSeq?: number
}

export interface SnapshotRequest {
  userId: string
  deviceId: string
  afterSeq?: number
  afterRow?: number
}

export interface SnapshotRecord {
  deleted?: boolean
  entity: SyncEntity
  syncUid: string
  payload: Record<string, unknown>
  updatedAt: number
  serverSeq: number
}

export interface SnapshotResponse {
  nextAfterSeq?: number
  nextAfterRow?: number
  userId: string
  latestServerSeq: number
  records: SnapshotRecord[]
}

export interface SyncScope {
  wsUrl: string
  userId: string
}

export interface SyncConfig extends SyncScope {
  enabled: boolean
  token: string
}
