import { fireEvent, render, screen } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { describe, expect, it, vi } from 'vitest';
import '../i18n';
import { TaskProvider, useTasks } from '../context/TaskContext';
import { TaskBell } from '../components/TaskBell';

/** Seeds one running task carrying an abort handle into the provider. */
function Seed({ abort }: { abort: () => void }) {
  const { addTask } = useTasks();
  return (
    <button onClick={() => addTask({ title: 'Download m v1', kind: 'download', progress: 10, abort })}>
      seed
    </button>
  );
}

describe('TaskBell', () => {
  it('should_call_abort_when_cancelling_a_running_task', async () => {
    const abort = vi.fn();
    render(
      <AntdApp>
        <TaskProvider>
          <Seed abort={abort} />
          <TaskBell />
        </TaskProvider>
      </AntdApp>,
    );

    fireEvent.click(screen.getByText('seed'));
    fireEvent.click(screen.getByRole('button', { name: 'tasks' }));
    fireEvent.click(await screen.findByRole('button', { name: 'Cancel' }));

    expect(abort).toHaveBeenCalledTimes(1);
  });
});
