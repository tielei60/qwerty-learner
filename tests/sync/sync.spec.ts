import { expect, test } from '@playwright/test'
import { spawn, spawnSync } from 'node:child_process'
import type { ChildProcess } from 'node:child_process'
import { once } from 'node:events'
import { mkdtempSync, rmSync } from 'node:fs'
import { connect } from 'node:net'
import { tmpdir } from 'node:os'
import path from 'node:path'

let server: ChildProcess
let tempDir: string

test.beforeAll(async () => {
  tempDir = mkdtempSync(path.join(tmpdir(), 'qwerty-sync-test-'))
  const binary = path.join(tempDir, process.platform === 'win32' ? 'sync-server.exe' : 'sync-server')
  const build = spawnSync('go', ['build', '-o', binary, './cmd/sync-server'], {
    cwd: path.resolve('sync-server'),
    encoding: 'utf8',
  })
  expect(build.status, build.stderr).toBe(0)
  server = spawn(binary, [], {
    env: {
      ...process.env,
      SYNC_DATA_DIR: path.join(tempDir, 'data'),
      SYNC_SQLITE_PATH: path.join(tempDir, 'data', 'shared.db'),
      SYNC_NATS_STORE_DIR: path.join(tempDir, 'data', 'jetstream'),
      SYNC_NATS_HOST: '127.0.0.1',
      SYNC_NATS_PORT: '18174',
      SYNC_WS_HOST: '127.0.0.1',
      SYNC_WS_PORT: '18175',
      SYNC_WS_ALLOWED_ORIGINS: 'http://127.0.0.1:18173',
      SYNC_AUTH_TOKEN: 'test-token',
    },
    stdio: 'ignore',
  })
  await expect
    .poll(
      () =>
        new Promise<boolean>((resolve) => {
          const socket = connect(18175, '127.0.0.1', () => {
            socket.destroy()
            resolve(true)
          })
          socket.once('error', () => resolve(false))
        }),
    )
    .toBe(true)
})

test.afterAll(async () => {
  if (server && server.exitCode === null && server.signalCode === null) {
    server.kill('SIGTERM')
    await once(server, 'exit')
  }
  if (tempDir) rmSync(tempDir, { recursive: true, force: true })
})

test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    if (!localStorage.getItem('qwerty_sync_config')) {
      localStorage.setItem('qwerty_sync_config', JSON.stringify({ enabled: false, wsUrl: 'ws://127.0.0.1:1', token: '', userId: 'test' }))
    }
    localStorage.setItem('hasSeenEnhancedPromotion', 'true')
    localStorage.setItem('pronunciation', JSON.stringify({ isOpen: false, isTransRead: false }))
    localStorage.setItem('keySoundsConfig', JSON.stringify({ isOpen: false }))
    localStorage.setItem('hintSoundsConfig', JSON.stringify({ isOpen: false }))
  })
  await page.goto('/')
  await expect(page.getByRole('button', { name: '开始', exact: true })).toBeVisible()
})

test('review config resolves portable IDs even when it arrives before the record', async ({ page }) => {
  const result = await page.evaluate(async () => {
    // Keep browser imports native when Playwright serializes this callback.
    const loadModule = new Function('modulePath', 'return import(modulePath)')
    const dbPath = '/src/utils/db/index.ts'
    const hooksPath = '/src/utils/sync/hooks.ts'
    const { db } = await loadModule(dbPath)
    const { applyRemoteEvent, serializeConfigValue } = await loadModule(hooksPath)
    const unrelated = { dict: 'local', index: 0, createTime: 1, isFinished: false, words: [] }
    const localId = await db.reviewRecords.add(unrelated)
    const record = { id: localId, dict: 'remote', index: 3, createTime: 2, isFinished: false, words: [{ name: 'hello', trans: ['你好'] }] }
    await applyRemoteEvent({
      sourceDeviceId: 'other',
      entity: 'config',
      syncUid: 'reviewModeInfo',
      op: 'UPDATE',
      payload: { value: JSON.stringify({ isReviewMode: true, reviewSyncUid: 'remote-review', reviewRecord: record }) },
    })
    const info = JSON.parse(localStorage.getItem('reviewModeInfo')!)
    // This is the same write used by reviewInfoAtom when continuing a review.
    await db.reviewRecords.put({ ...info.reviewRecord, index: 4 })
    const portable = JSON.parse(await serializeConfigValue('reviewModeInfo', JSON.stringify(info)))
    const preserved = await db.reviewRecords.get(localId)
    await applyRemoteEvent({
      sourceDeviceId: 'other',
      entity: 'config',
      syncUid: 'reviewModeInfo',
      op: 'UPDATE',
      payload: { value: JSON.stringify({ isReviewMode: true, reviewRecord: record }) },
    })
    return {
      localId,
      mappedId: info.reviewRecord.id,
      preserved,
      portable,
      legacyId: JSON.parse(localStorage.getItem('reviewModeInfo')!).reviewRecord.id,
    }
  })
  expect(result.mappedId).not.toBe(result.localId)
  expect(result.preserved.dict).toBe('local')
  expect(result.portable.reviewRecord.id).toBeUndefined()
  expect(result.portable.reviewSyncUid).toBe('remote-review')
  expect(result.legacyId).toBeUndefined()
})

test('historical duplicates retain distinct identities and review configs omit local IDs', async ({ page }) => {
  const result = await page.evaluate(async () => {
    // Keep browser imports native when Playwright serializes this callback.
    const loadModule = new Function('modulePath', 'return import(modulePath)')
    const dbPath = '/src/utils/db/index.ts'
    const backfillPath = '/src/utils/sync/backfill.ts'
    const metaPath = '/src/utils/sync/metaDb.ts'
    const { db } = await loadModule(dbPath)
    const { scanAndEnqueueHistoricalData } = await loadModule(backfillPath)
    const { listOutbox, getSyncUidByLocalId } = await loadModule(metaPath)
    const word = { word: 'a', dict: 'test', chapter: 0, timeStamp: 100, timing: [], wrongCount: 0, mistakes: {} }
    const first = await db.wordRecords.add({ ...word })
    const second = await db.wordRecords.add({ ...word })
    const review = { dict: 'test', index: 0, createTime: 100, isFinished: false, words: [{ name: 'a', trans: [] }] }
    const reviewId = await db.reviewRecords.add({ ...review })
    await db.reviewRecords.add({ ...review })
    localStorage.setItem('reviewModeInfo', JSON.stringify({ isReviewMode: false, reviewRecord: { ...review, id: reviewId } }))
    await scanAndEnqueueHistoricalData('duplicates')
    const repeat = await scanAndEnqueueHistoricalData('duplicates')
    const events = (await listOutbox()).map((row: { event: unknown }) => row.event)
    return {
      first: await getSyncUidByLocalId('wordRecords', first),
      second: await getSyncUidByLocalId('wordRecords', second),
      repeat,
      events,
    }
  })
  expect(result.first).not.toBe(result.second)
  expect(result.repeat.queued).toBe(0)
  expect(result.events.filter((event: any) => event.entity === 'wordRecords')).toHaveLength(2)
  expect(result.events.filter((event: any) => event.entity === 'reviewRecords')).toHaveLength(2)
  const config = result.events.find((event: any) => event.syncUid === 'reviewModeInfo') as any
  expect(JSON.parse(config.payload.value).reviewRecord.id).toBeUndefined()
  expect(JSON.parse(config.payload.value).reviewSyncUid).toBeTruthy()
})

test('switching servers and resetting server sequences do not suppress new events', async ({ page }) => {
  const result = await page.evaluate(async () => {
    // Keep browser imports native when Playwright serializes this callback.
    const loadModule = new Function('modulePath', 'return import(modulePath)')
    const metaPath = '/src/utils/sync/metaDb.ts'
    const enginePath = '/src/utils/sync/index.ts'
    const clientPath = '/src/utils/sync/natsWsClient.ts'
    const dbPath = '/src/utils/db/index.ts'
    const meta = await loadModule(metaPath)
    const engine = await loadModule(enginePath)
    const { NatsWsClient } = await loadModule(clientPath)
    const { db } = await loadModule(dbPath)
    const scope = { wsUrl: 'ws://127.0.0.1:18175', userId: 'cursor' }
    const oldScope = { ...scope, wsUrl: 'ws://127.0.0.1:18176' }
    await meta.setLastServerSeq(oldScope, 100)
    const fresh = await meta.getLastServerSeq(scope)
    const otherUser = await meta.getLastServerSeq({ ...oldScope, userId: 'other-user' })
    await meta.syncMetaDb.kv.put({ key: 'backfilled:cursor', value: '1' })
    const publisher: any = await new Promise((resolve) => {
      const client = new NatsWsClient({ url: scope.wsUrl, token: 'test-token', onConnected: () => resolve(client) })
      client.start()
    })
    const send = async (n: number) => {
      await publisher.request(
        'sync.up.cursor',
        JSON.stringify({
          eventId: `cursor-${n}`,
          userId: 'cursor',
          sourceDeviceId: 'other',
          entity: 'wordRecords',
          op: 'CREATE',
          syncUid: `cursor-word-${n}`,
          payload: { word: `word-${n}`, dict: 'test', chapter: 0, timeStamp: n, timing: [], wrongCount: 0, mistakes: {} },
          clientTs: Date.now(),
        }),
      )
    }
    const waitForCount = async (count: number) => {
      for (let i = 0; i < 100; i++) {
        if ((await db.wordRecords.count()) === count) return
        await new Promise((resolve) => setTimeout(resolve, 20))
      }
      throw new Error(`did not receive ${count} records`)
    }
    await send(1)
    engine.applySyncConfig({ ...scope, enabled: true, token: 'test-token' })
    await waitForCount(1)
    await send(2)
    await waitForCount(2)
    await engine.triggerManualSync()
    // Simulate a saved cursor from before the server database was recreated.
    await meta.setLastServerSeq(scope, 100)
    await engine.triggerManualSync()
    const reset = await meta.getLastServerSeq(scope)
    await send(3)
    await waitForCount(3)
    publisher.close()
    engine.applySyncConfig({ ...scope, enabled: false, token: 'test-token' })
    return { fresh, otherUser, reset, oldCursor: await meta.getLastServerSeq(oldScope), count: await db.wordRecords.count() }
  })
  expect(result).toEqual({ fresh: 0, otherUser: 0, reset: 2, oldCursor: 100, count: 3 })
})

test('Restart replaces the previous checkpoint before resuming after reload', async ({ page }) => {
  const checkpointIndex = () =>
    page.evaluate(() => JSON.parse(localStorage.getItem('unfinishedPractice') ?? 'null')?.progress?.chapterData.index)
  await page.keyboard.press('Enter')
  await page.keyboard.type('cancel', { delay: 15 })
  await expect.poll(checkpointIndex).toBe(1)
  await page.keyboard.type('explosive', { delay: 15 })
  await expect.poll(checkpointIndex).toBe(2)
  await page.getByRole('button', { name: '暂停', exact: true }).hover()
  await page.getByRole('button', { name: '重新开始', exact: true }).click()
  await expect.poll(checkpointIndex).toBeUndefined()
  await page.keyboard.type('cancel', { delay: 15 })
  await expect.poll(checkpointIndex).toBe(1)
  await page.reload()
  await page.getByRole('button', { name: '开始', exact: true }).click()
  await expect.poll(async () => (await page.locator('[lang="en"]').innerText()).replace(/\s/g, '')).toMatch(/^explosive/)
})
