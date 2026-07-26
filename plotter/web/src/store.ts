import { create } from 'zustand'
import { persist } from 'zustand/middleware'

import type { Language } from './i18n'

const sameIds = (left: string[], right: string[]) =>
  left.length === right.length && left.every((value, index) => value === right[index])

type AppState = {
  selectedMetricsRunId: string
  selectedCategory: string
  selectedBacktestId: string
  selectedWfaRunId: string
  parameterMatrixSearchSource: string
  language: Language
  benchmarkVisible: boolean
  shareMosaicMode: boolean
  batchIds: string[]
  serverSessionId: string
  setSelectedMetricsRunId: (value: string) => void
  setSelectedCategory: (value: string) => void
  setSelectedBacktestId: (value: string) => void
  setSelectedWfaRunId: (value: string) => void
  setParameterMatrixSearchSource: (value: string) => void
  setLanguage: (value: Language) => void
  setBenchmarkVisible: (value: boolean) => void
  setShareMosaicMode: (value: boolean) => void
  setServerSessionId: (value: string) => void
  addBatchId: (value: string) => void
  removeBatchId: (value: string) => void
  replaceBatchIds: (value: string[]) => void
}

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      selectedMetricsRunId: '',
      selectedCategory: 'top_3_sharpe',
      selectedBacktestId: '',
      selectedWfaRunId: '',
      parameterMatrixSearchSource: 'all_existing_results',
      language: 'zh-Hant',
      benchmarkVisible: true,
      shareMosaicMode: false,
      batchIds: [],
      serverSessionId: '',
      setSelectedMetricsRunId: (value) =>
        set((state) => (state.selectedMetricsRunId === value ? state : { selectedMetricsRunId: value })),
      setSelectedCategory: (value) =>
        set((state) => (state.selectedCategory === value ? state : { selectedCategory: value })),
      setSelectedBacktestId: (value) =>
        set((state) => (state.selectedBacktestId === value ? state : { selectedBacktestId: value })),
      setSelectedWfaRunId: (value) =>
        set((state) => (state.selectedWfaRunId === value ? state : { selectedWfaRunId: value })),
      setParameterMatrixSearchSource: (value) =>
        set((state) => (state.parameterMatrixSearchSource === value ? state : { parameterMatrixSearchSource: value })),
      setLanguage: (value) =>
        set((state) => (state.language === value ? state : { language: value })),
      setBenchmarkVisible: (value) =>
        set((state) => (state.benchmarkVisible === value ? state : { benchmarkVisible: value })),
      setShareMosaicMode: (value) =>
        set((state) => (state.shareMosaicMode === value ? state : { shareMosaicMode: value })),
      setServerSessionId: (value) =>
        set((state) => (state.serverSessionId === value ? state : { serverSessionId: value })),
      addBatchId: (value) =>
        set((state) => ({
          batchIds: state.batchIds.includes(value)
            ? state.batchIds
            : [value, ...state.batchIds].slice(0, 8),
        })),
      removeBatchId: (value) =>
        set((state) => ({
          batchIds: state.batchIds.filter((item) => item !== value),
        })),
      replaceBatchIds: (value) =>
        set((state) => {
          const nextBatchIds = Array.from(new Set(value)).slice(0, 8)
          return sameIds(state.batchIds, nextBatchIds) ? state : { batchIds: nextBatchIds }
        }),
    }),
    {
      name: 'lo2cin4bt-app-store-v5',
      version: 11,
      migrate: (persistedState: any, version) => {
        if (!persistedState || version === 11) {
          return persistedState
        }
        return {
          ...persistedState,
          selectedCategory: 'top_3_sharpe',
          selectedMetricsRunId: '',
          selectedBacktestId: '',
          selectedWfaRunId: '',
          parameterMatrixSearchSource: 'all_existing_results',
          language: 'zh-Hant',
          benchmarkVisible: true,
          shareMosaicMode: false,
          batchIds: [],
          serverSessionId: '',
        }
      },
      partialize: (state) => ({
        selectedMetricsRunId: state.selectedMetricsRunId,
        selectedCategory: state.selectedCategory,
        selectedBacktestId: state.selectedBacktestId,
        selectedWfaRunId: state.selectedWfaRunId,
        parameterMatrixSearchSource: state.parameterMatrixSearchSource,
        language: state.language,
        benchmarkVisible: state.benchmarkVisible,
        shareMosaicMode: state.shareMosaicMode,
        batchIds: state.batchIds,
        serverSessionId: state.serverSessionId,
      }),
    },
  ),
)
