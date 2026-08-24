import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react';

export type TaskStatus = 'running' | 'success' | 'error';

export interface UiTask {
  id: string;
  title: string;
  kind: 'upload' | 'load' | 'other';
  status: TaskStatus;
  /** 0-100 when known. */
  progress?: number;
  detail?: string;
  createdAt: number;
}

interface TaskContextValue {
  tasks: UiTask[];
  addTask: (task: Omit<UiTask, 'id' | 'createdAt' | 'status'> & { status?: TaskStatus }) => string;
  updateTask: (id: string, patch: Partial<Omit<UiTask, 'id'>>) => void;
  dismissTask: (id: string) => void;
  clearFinished: () => void;
}

const TaskContext = createContext<TaskContextValue | null>(null);

let nextId = 1;

export function TaskProvider({ children }: { children: ReactNode }) {
  const [tasks, setTasks] = useState<UiTask[]>([]);

  const addTask: TaskContextValue['addTask'] = useCallback((task) => {
    const id = `task-${nextId++}`;
    setTasks((prev) => [
      { ...task, id, status: task.status ?? 'running', createdAt: Date.now() },
      ...prev,
    ]);
    return id;
  }, []);

  const updateTask: TaskContextValue['updateTask'] = useCallback((id, patch) => {
    setTasks((prev) => prev.map((t) => (t.id === id ? { ...t, ...patch } : t)));
  }, []);

  const dismissTask = useCallback((id: string) => {
    setTasks((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const clearFinished = useCallback(() => {
    setTasks((prev) => prev.filter((t) => t.status === 'running'));
  }, []);

  const value = useMemo(
    () => ({ tasks, addTask, updateTask, dismissTask, clearFinished }),
    [tasks, addTask, updateTask, dismissTask, clearFinished],
  );

  return <TaskContext.Provider value={value}>{children}</TaskContext.Provider>;
}

export function useTasks(): TaskContextValue {
  const ctx = useContext(TaskContext);
  if (!ctx) throw new Error('useTasks must be used within TaskProvider');
  return ctx;
}
