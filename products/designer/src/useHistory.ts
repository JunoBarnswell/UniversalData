import { useCallback, useState } from 'react';
export function useHistory<T>(initial: () => T) {
  const [history, setHistory] = useState(() => ({ past: [] as T[], value: initial(), future: [] as T[] }));
  const edit = useCallback((update: (value: T) => T) => setHistory(current => ({ past: [...current.past.slice(-49), current.value], value: update(current.value), future: [] })), []);
  const reset = useCallback((value: T) => setHistory({ past: [], value, future: [] }), []);
  const undo = useCallback(() => setHistory(current => current.past.length ? { past: current.past.slice(0, -1), value: current.past.at(-1)!, future: [current.value, ...current.future] } : current), []);
  const redo = useCallback(() => setHistory(current => current.future.length ? { past: [...current.past, current.value], value: current.future[0], future: current.future.slice(1) } : current), []);
  return { value: history.value, edit, reset, undo, redo, canUndo: history.past.length > 0, canRedo: history.future.length > 0 };
}
