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
}

export interface SnapshotRecord {
  entity: SyncEntity
  syncUid: string
  payload: Record<string, unknown>
  updatedAt: number
  serverSeq: number
}

export interface SnapshotResponse {
  userId: string
  latestServerSeq: number
  records: SnapshotRecord[]
}

export interface SyncConfig {
  enabled: boolean
  wsUrl: string
  token: string
  userId: string
}
