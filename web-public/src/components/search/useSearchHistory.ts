import { useCallback, useState } from "react";

import {
  addSearchHistoryEntry,
  clearSearchHistory,
  readSearchHistory,
  SearchHistoryEntry,
} from "./searchHistory";

export interface SearchHistoryState {
  entries: SearchHistoryEntry[];
  addEntry: (entry: SearchHistoryEntry) => void;
  clearEntries: () => void;
}

export function useSearchHistory(): SearchHistoryState {
  const [entries, setEntries] = useState<SearchHistoryEntry[]>(() => readSearchHistory());

  const addEntry = useCallback((entry: SearchHistoryEntry) => {
    setEntries(addSearchHistoryEntry(entry));
  }, []);

  const clearEntries = useCallback(() => {
    clearSearchHistory();
    setEntries([]);
  }, []);

  return { entries, addEntry, clearEntries };
}
