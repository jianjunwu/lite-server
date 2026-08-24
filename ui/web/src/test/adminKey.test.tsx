import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { describe, expect, it } from 'vitest';
import '../i18n';
import { AdminKeyProvider, useRequestAdminKey } from '../context/AdminKeyContext';

let requester: ((instanceId: string) => Promise<string | null>) | null = null;

function Capture() {
  requester = useRequestAdminKey();
  return null;
}

function renderProvider() {
  return render(
    <AntdApp>
      <AdminKeyProvider>
        <Capture />
      </AdminKeyProvider>
    </AntdApp>,
  );
}

async function submitKey(key: string) {
  const input = await screen.findByTestId('admin-key-input');
  fireEvent.change(input, { target: { value: key } });
  fireEvent.click(screen.getByRole('button', { name: /save & retry/i }));
}

describe('AdminKeyProvider concurrency', () => {
  it('should_reuse_the_same_prompt_for_concurrent_requests_of_one_instance', async () => {
    renderProvider();
    const p1 = requester!('prod');
    const p2 = requester!('prod');
    // The second call must not overwrite the first one's resolver.
    expect(p2).toBe(p1);
    await submitKey('k1');
    await expect(p1).resolves.toBe('k1');
    await expect(p2).resolves.toBe('k1');
  });

  it('should_queue_prompts_for_different_instances', async () => {
    renderProvider();
    const p1 = requester!('a');
    const p2 = requester!('b');
    await screen.findByTestId('admin-key-input');
    expect(screen.getByText('a')).toBeInTheDocument();
    await submitKey('key-a');
    await expect(p1).resolves.toBe('key-a');
    // After the first prompt closes, the queued instance gets its turn.
    await waitFor(() => expect(screen.getByText('b')).toBeInTheDocument());
    await submitKey('key-b');
    await expect(p2).resolves.toBe('key-b');
  });

  it('should_resolve_all_waiting_callers_when_cancelled', async () => {
    renderProvider();
    const p1 = requester!('prod');
    const p2 = requester!('prod');
    await screen.findByTestId('admin-key-input');
    fireEvent.click(screen.getByRole('button', { name: /cancel/i }));
    await expect(p1).resolves.toBeNull();
    await expect(p2).resolves.toBeNull();
  });
});
