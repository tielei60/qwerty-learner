import { TypingStateActionType } from '../store'
import type { TypingStateAction } from '../store'
import type { ChapterData, TimerData, TypingState } from '../store/type'
import type { WordWithIndex } from '@/typings'
import { getLocalIdBySyncUid, getSyncUidByLocalId } from '@/utils/sync/metaDb'
import { useEffect, useRef, useState } from 'react'

const storageKey = 'unfinishedPractice'

type Checkpoint = {
  dict: string
  chapter: number
  progress: {
    chapterData: Omit<ChapterData, 'words' | 'wordRecordIds'>
    timerData: TimerData
    order: Array<[number, string]>
    wordUids: string[]
  } | null
}

function readCheckpoint(): Checkpoint | null {
  try {
    return JSON.parse(localStorage.getItem(storageKey) ?? 'null') as Checkpoint | null
  } catch {
    return null
  }
}

function storeCheckpoint(checkpoint: Checkpoint) {
  localStorage.setItem(storageKey, JSON.stringify(checkpoint))
}

export function usePracticeResume({
  state,
  dispatch,
  dict,
  chapter,
  words,
  review,
  selectChapter,
}: {
  state: TypingState
  dispatch: (action: TypingStateAction) => void
  dict: string
  chapter: number
  words: WordWithIndex[]
  review: boolean
  selectChapter: (dict: string, chapter: number) => void
}) {
  const [available, setAvailable] = useState(readCheckpoint)
  const [requested, setRequested] = useState<Checkpoint | null>(null)
  const saved = useRef({ key: '', count: 0, index: 0, finished: false })
  const generation = useRef(0)
  const key = JSON.stringify([dict, chapter])

  useEffect(() => {
    const receive = (event: StorageEvent) => {
      if (event.key === storageKey) setAvailable(readCheckpoint())
    }
    window.addEventListener('storage', receive)
    return () => window.removeEventListener('storage', receive)
  }, [])

  useEffect(() => {
    if (review || state.practiceKey !== key) return
    const count = state.chapterData.wordCount
    const { index } = state.chapterData
    const previous = saved.current
    if (previous.key === key && previous.count === count && previous.finished === state.isFinished) return
    if (state.isSavingRecord && !state.isFinished) return
    if (!state.isFinished && count > 0 && (previous.key !== key ? index === 0 : previous.index === index)) return
    saved.current = { key, count, index, finished: state.isFinished }
    const version = ++generation.current
    if (state.isFinished) {
      const stored = readCheckpoint()
      if (stored?.dict === dict && stored.chapter === chapter) {
        storeCheckpoint({ dict, chapter, progress: null })
        setAvailable(null)
      }
      return
    }
    if (!count) return

    const { words: orderedWords, wordRecordIds, ...chapterData } = state.chapterData
    const { time } = state.timerData
    const inputs = chapterData.correctCount + chapterData.wrongCount
    void Promise.all(wordRecordIds.map((id) => getSyncUidByLocalId('wordRecords', id)))
      .then((ids) => {
        if (version !== generation.current) return
        storeCheckpoint({
          dict,
          chapter,
          progress: {
            chapterData,
            timerData: {
              time,
              accuracy: inputs ? Math.round((100 * chapterData.correctCount) / inputs) : 0,
              wpm: time ? Math.round((60 * count) / time) : 0,
            },
            order: orderedWords.map(({ index, name }) => [index, name]),
            wordUids: ids.filter((id): id is string => id !== undefined),
          },
        })
        setAvailable(null)
      })
      .catch(console.error)
  }, [state, key, review, dict, chapter])

  useEffect(() => {
    const progress = requested?.progress
    if (!progress || review || requested.dict !== dict || requested.chapter !== chapter || state.practiceKey !== key || !words.length)
      return
    const byIndex = new Map(words.map((word) => [word.index, word]))
    const orderedWords = progress.order.map(([index, name]) => {
      const word = byIndex.get(index)
      return word?.name === name ? word : undefined
    })
    if (orderedWords.length !== words.length || orderedWords.some((word) => !word)) {
      setRequested(null)
      return
    }

    let cancelled = false
    void Promise.all(progress.wordUids.map((uid) => getLocalIdBySyncUid('wordRecords', uid)))
      .then((ids) => {
        if (cancelled) return
        saved.current = { key, count: progress.chapterData.wordCount, index: progress.chapterData.index, finished: false }
        dispatch({
          type: TypingStateActionType.RESTORE_PRACTICE,
          payload: {
            chapterData: {
              ...progress.chapterData,
              words: orderedWords.filter((word): word is WordWithIndex => word !== undefined),
              wordRecordIds: ids.filter((id): id is number => id !== undefined),
            },
            timerData: progress.timerData,
          },
        })
        setRequested(null)
        setAvailable(null)
      })
      .catch(console.error)
    return () => {
      cancelled = true
    }
  }, [requested, review, dict, chapter, key, state.practiceKey, words, dispatch])

  return available?.progress && !requested
    ? () => {
        setRequested(available)
        selectChapter(available.dict, available.chapter)
      }
    : null
}
