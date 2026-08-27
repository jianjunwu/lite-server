/** Audit repro tests (feat/admin-enhancement review). */
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ConfigEditor } from '../components/config/ConfigEditor';
import type { ModelConfigResponse } from '../api/config';

vi.mock('../context/useEffectiveRole', () => ({
  useCanInstance: () => () => true,
}));

function makeData(instanceTag: string, maxBatchSize: number): ModelConfigResponse {
  return {
    model: 'm',
    version: '1',
    config: { max_batch_size: maxBatchSize },
    has_file: true,
    redacted: [],
    etag: `etag-${instanceTag}`,
    loaded_at: null,
  };
}

function renderEditor(ui: React.ReactElement) {
  const queryClient = new QueryClient();
  return render(
    <QueryClientProvider client={queryClient}>
      <AntdApp>{ui}</AntdApp>
    </QueryClientProvider>,
  );
}

describe('ConfigEditor identity changes', () => {
  it('drops the draft when the underlying config identity changes (instance switch)', () => {
    // Audit UI-H1: switching the global instance only changes a search param —
    // VersionDetailPage does NOT remount and ConfigEditor is rendered without
    // a key, so `editing`/`draft` state survives. If the new instance's config
    // is already in the react-query cache, `data` swaps to instance B while
    // the draft still holds instance A's edits; Save then sends A's changes
    // with B's VALID etag — the optimistic lock cannot fire.
    const dataA = makeData('a', 16);
    const dataB = makeData('b', 99);

    const { rerender } = renderEditor(
      <ConfigEditor instanceId="inst-a" model="m" version="1" data={dataA} onEditingChange={() => {}} />,
    );

    // Enter edit mode and dirty the draft on instance A.
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const input = screen
      .getAllByRole('spinbutton')
      .find((el) => (el as HTMLInputElement).value === '16')!;
    fireEvent.change(input, { target: { value: '32' } });
    expect(screen.getByRole('button', { name: 'Save' })).toBeEnabled();

    // Instance switch: same route, same component instance, new data/etag.
    rerender(
      <QueryClientProvider client={new QueryClient()}>
        <AntdApp>
          <ConfigEditor instanceId="inst-b" model="m" version="1" data={dataB} onEditingChange={() => {}} />
        </AntdApp>
      </QueryClientProvider>,
    );

    // The edit session must not survive an identity change: a live Save
    // button here means A's draft would be submitted against B's etag.
    expect(
      screen.queryByRole('button', { name: 'Save' }),
    ).toBeNull();
  });
});

describe('ConfigEditor set-first editing', () => {
  function makeSetData(): ModelConfigResponse {
    return {
      model: 'm',
      version: '1',
      config: { max_batch_size: 16, accelerator: 'cpu' },
      has_file: true,
      redacted: [],
      etag: 'e1',
      loaded_at: null,
    };
  }

  function renderFull() {
    return renderEditor(
      <ConfigEditor instanceId="i" model="m" version="1" data={makeSetData()} onEditingChange={() => {}} />,
    );
  }

  it('should_show_only_set_fields_in_edit_mode_and_add_unset_fields_on_demand', async () => {
    renderFull();
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    expect(screen.getByText('max_batch_size')).toBeTruthy();
    expect(screen.getByText('accelerator')).toBeTruthy();
    // Unset schema fields are not dumped onto the form…
    expect(screen.queryByText('batch_timeout')).toBeNull();
    // …but can be pulled in from the group's add-field picker.
    fireEvent.mouseDown(screen.getAllByRole('combobox')[0]);
    fireEvent.click(document.querySelector('.ant-select-item-option[title="batch_timeout"]')!);
    // A new empty row appears (a second numeric control next to max_batch_size).
    await waitFor(() => expect(screen.getAllByRole('spinbutton')).toHaveLength(2));
    expect(screen.getAllByText('batch_timeout').length).toBeGreaterThan(0);
  });

  it('should_show_the_server_default_as_placeholder_for_an_unset_field', async () => {
    renderFull();
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    fireEvent.mouseDown(screen.getAllByRole('combobox')[0]);
    fireEvent.click(document.querySelector('.ant-select-item-option[title="batch_timeout"]')!);
    await waitFor(() =>
      expect(document.querySelector('input[placeholder*="Default"]')).not.toBeNull(),
    );
  });

  it('should_explain_a_field_via_a_tooltip', async () => {
    renderFull();
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    fireEvent.mouseEnter(screen.getByTestId('field-desc-max_batch_size'));
    expect(await screen.findByText(/maximum requests merged/i)).toBeTruthy();
  });

  it('should_count_unsaved_changes_in_a_sticky_bar_and_mark_changed_rows', () => {
    renderFull();
    fireEvent.click(screen.getByRole('button', { name: 'Edit' }));
    const input = screen
      .getAllByRole('spinbutton')
      .find((el) => (el as HTMLInputElement).value === '16')!;
    fireEvent.change(input, { target: { value: '32' } });
    expect(screen.getByText('1 unsaved change')).toBeTruthy();
    expect(document.querySelector('[data-changed="true"]')).not.toBeNull();
    // The single Save lives in the sticky bar — no duplicate at the top.
    expect(screen.getAllByRole('button', { name: 'Save' })).toHaveLength(1);
  });
});
