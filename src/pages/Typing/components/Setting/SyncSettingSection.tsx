import styles from './index.module.css'
import type { SyncStatusSnapshot } from '@/utils/sync'
import { applySyncConfig, loadSyncConfig, subscribeSyncStatus, syncHistoricalData, triggerManualSync } from '@/utils/sync'
import { Switch } from '@headlessui/react'
import { useCallback, useEffect, useState } from 'react'

export default function SyncSettingSection() {
  const initial = loadSyncConfig()
  const [enabled, setEnabled] = useState(initial.enabled)
  const [wsUrl, setWsUrl] = useState(initial.wsUrl || 'ws://127.0.0.1:9322')
  const [token, setToken] = useState(initial.token)
  const [userId, setUserId] = useState(initial.userId)
  const [isSyncing, setIsSyncing] = useState(false)
  const [isBackfilling, setIsBackfilling] = useState(false)
  const [savedHint, setSavedHint] = useState('')
  const [status, setStatus] = useState<SyncStatusSnapshot>({
    enabled: initial.enabled,
    connected: false,
    pendingOutbox: 0,
    lastServerSeq: 0,
    deviceId: '',
  })

  useEffect(() => {
    return subscribeSyncStatus(setStatus)
  }, [])

  const onToggleEnabled = useCallback(
    (checked: boolean) => {
      setEnabled(checked)
      if (!checked) {
        applySyncConfig({ enabled: false, wsUrl: wsUrl.trim(), token: token.trim(), userId: userId.trim() })
        setSavedHint('已关闭多端同步')
      } else if (wsUrl.trim() && userId.trim()) {
        applySyncConfig({ enabled: true, wsUrl: wsUrl.trim(), token: token.trim(), userId: userId.trim() })
        setSavedHint('已开启并尝试连接')
      }
    },
    [token, userId, wsUrl],
  )

  const onSaveAndConnect = useCallback(() => {
    const trimmedUrl = wsUrl.trim()
    const trimmedUser = userId.trim()
    if (!trimmedUrl || !trimmedUser) {
      setSavedHint('请填写 WS 地址和用户 ID')
      return
    }
    setEnabled(true)
    applySyncConfig({
      enabled: true,
      wsUrl: trimmedUrl,
      token: token.trim(),
      userId: trimmedUser,
    })
    setSavedHint('配置已保存')
  }, [token, userId, wsUrl])

  const onManualSync = useCallback(async () => {
    setIsSyncing(true)
    try {
      await triggerManualSync()
    } finally {
      setIsSyncing(false)
    }
  }, [])

  const onSyncHistory = useCallback(async () => {
    setIsBackfilling(true)
    try {
      const res = await syncHistoricalData()
      setSavedHint(res.queued > 0 ? `已扫描 ${res.scanned} 条，上报 ${res.queued} 条历史记录` : '历史数据已同步，无需重复上报')
    } finally {
      setIsBackfilling(false)
    }
  }, [])

  const statusDotClass = !status.enabled
    ? 'bg-gray-400'
    : status.connected
    ? status.pendingOutbox > 0
      ? 'bg-amber-400'
      : 'bg-emerald-500'
    : 'bg-rose-500'

  const statusText = !status.enabled
    ? '未开启'
    : status.connected
    ? status.pendingOutbox > 0
      ? `已连接 · 待同步 ${status.pendingOutbox} 条`
      : `已同步 (seq ${status.lastServerSeq})`
    : `未连接${status.pendingOutbox > 0 ? ` · 离线缓存 ${status.pendingOutbox} 条` : ''}`

  return (
    <div className={styles.section}>
      <div className="flex w-full items-center justify-between">
        <span className={styles.sectionLabel}>多端同步（消息队列）</span>
        <span className="inline-flex items-center gap-1.5 rounded-full bg-gray-100 px-2.5 py-0.5 text-xs text-gray-600 dark:bg-gray-700 dark:text-gray-300">
          <span className={`h-2 w-2 rounded-full ${statusDotClass}`} />
          {statusText}
        </span>
      </div>
      <span className={styles.sectionDescription}>
        通过内嵌 NATS WebSocket 在网页端与桌面客户端间异步同步学习记录和配置，两端仍保留各自的本地存储，断网不受影响。
      </span>

      <div className={styles.switchBlock}>
        <Switch checked={enabled} onChange={onToggleEnabled} className="switch-root">
          <span aria-hidden="true" className="switch-thumb" />
        </Switch>
        <span className="text-right text-xs font-normal leading-tight text-gray-600 dark:text-gray-300">
          {`多端同步已${enabled ? '开启' : '关闭'}`}
        </span>
      </div>

      <div className="flex w-full flex-col gap-3 pl-4">
        <div className="flex flex-col gap-1">
          <label className="text-left text-xs font-medium text-gray-600 dark:text-gray-300">NATS WebSocket 地址</label>
          <input
            type="text"
            value={wsUrl}
            onChange={(e) => setWsUrl(e.target.value)}
            placeholder="ws://127.0.0.1:9322"
            className="w-full rounded-md border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none dark:border-gray-600 dark:bg-gray-900 dark:text-gray-200"
          />
        </div>

        <div className="grid grid-cols-2 gap-3">
          <div className="flex flex-col gap-1">
            <label className="text-left text-xs font-medium text-gray-600 dark:text-gray-300">用户 ID（两端保持一致）</label>
            <input
              type="text"
              value={userId}
              onChange={(e) => setUserId(e.target.value)}
              placeholder="例如 my-user"
              className="w-full rounded-md border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none dark:border-gray-600 dark:bg-gray-900 dark:text-gray-200"
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-left text-xs font-medium text-gray-600 dark:text-gray-300">认证 Token</label>
            <input
              type="password"
              value={token}
              onChange={(e) => setToken(e.target.value)}
              placeholder="SYNC_AUTH_TOKEN"
              className="w-full rounded-md border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-700 focus:border-indigo-400 focus:outline-none dark:border-gray-600 dark:bg-gray-900 dark:text-gray-200"
            />
          </div>
        </div>

        <div className="flex items-center gap-3 pt-1">
          <button type="button" className="my-btn-primary text-sm" onClick={onSaveAndConnect}>
            保存并连接
          </button>
          <button
            type="button"
            className="rounded-md border border-gray-200 px-3 py-1.5 text-sm text-gray-600 transition-colors hover:bg-gray-100 disabled:opacity-50 dark:border-gray-600 dark:text-gray-300 dark:hover:bg-gray-700"
            onClick={onManualSync}
            disabled={!status.connected || isSyncing}
          >
            {isSyncing ? '同步中...' : '立即同步'}
          </button>
          <button
            type="button"
            className="rounded-md border border-gray-200 px-3 py-1.5 text-sm text-gray-600 transition-colors hover:bg-gray-100 disabled:opacity-50 dark:border-gray-600 dark:text-gray-300 dark:hover:bg-gray-700"
            onClick={onSyncHistory}
            disabled={!status.enabled || isBackfilling}
          >
            {isBackfilling ? '扫描中...' : '同步历史数据'}
          </button>
          {savedHint && <span className="text-xs text-gray-500 dark:text-gray-400">{savedHint}</span>}
        </div>
      </div>
    </div>
  )
}
