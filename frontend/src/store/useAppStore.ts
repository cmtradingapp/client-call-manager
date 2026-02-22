import { create } from 'zustand';

import type { CallStatusType, ClientDetail, FilterParams } from '../types';

interface AppState {
  // Filters
  filters: FilterParams;
  setFilters: (patch: Partial<FilterParams>) => void;
  resetFilters: () => void;

  // Search results
  results: ClientDetail[];
  setResults: (results: ClientDetail[]) => void;
  isSearching: boolean;
  setIsSearching: (v: boolean) => void;
  searchError: string | null;
  setSearchError: (e: string | null) => void;

  // Row selection
  selectedIds: Set<string>;
  toggleSelected: (id: string) => void;
  selectAll: () => void;
  deselectAll: () => void;

  // Per-client call statuses
  callStatuses: Record<string, CallStatusType>;
  setCallStatus: (clientId: string, status: CallStatusType) => void;
  resetCallStatuses: () => void;
  isCalling: boolean;
  setIsCalling: (v: boolean) => void;
}

const defaultFilters: FilterParams = {};

export const useAppStore = create<AppState>((set) => ({
  // Filters
  filters: defaultFilters,
  setFilters: (patch) =>
    set((state) => ({ filters: { ...state.filters, ...patch } })),
  resetFilters: () => set({ filters: defaultFilters }),

  // Results — also clears selection and call statuses when new results arrive
  results: [],
  setResults: (results) =>
    set({ results, selectedIds: new Set(), callStatuses: {} }),
  isSearching: false,
  setIsSearching: (v) => set({ isSearching: v }),
  searchError: null,
  setSearchError: (e) => set({ searchError: e }),

  // Selection
  selectedIds: new Set(),
  toggleSelected: (id) =>
    set((state) => {
      const next = new Set(state.selectedIds);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return { selectedIds: next };
    }),
  selectAll: () =>
    set((state) => ({
      selectedIds: new Set(state.results.map((r) => r.client_id)),
    })),
  deselectAll: () => set({ selectedIds: new Set() }),

  // Call statuses
  callStatuses: {},
  setCallStatus: (clientId, status) =>
    set((state) => ({
      callStatuses: { ...state.callStatuses, [clientId]: status },
    })),
  resetCallStatuses: () => set({ callStatuses: {} }),
  isCalling: false,
  setIsCalling: (v) => set({ isCalling: v }),
}));
