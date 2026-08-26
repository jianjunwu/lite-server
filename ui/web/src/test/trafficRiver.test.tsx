import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { modelOps } from '../api/mutations';
import type { VersionInfo } from '../api/types';
import { TrafficRiver } from '../components/TrafficRiver';
import { InstanceProvider } from '../context/InstanceContext';

vi.mock('../api/mutations', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../api/mutations')>();
  return {
    ...mod,
    modelOps: { ...mod.modelOps, setRouting: vi.fn().mockResolvedValue({ success: true }) },
  };
});

const setRouting = vi.mocked(modelOps.setRouting);

function version(version: string, weight: number, active = false): VersionInfo {
  return { version, status: 'READY', active, weight, workers: { ready: 1, total: 1 }, loaded_at: null };
}

function renderRiver(ui: React.ReactElement) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <MemoryRouter initialEntries={['/?i=prod']}>
      <QueryClientProvider client={queryClient}>
        <AntdApp>
          <InstanceProvider>{ui}</InstanceProvider>
        </AntdApp>
      </QueryClientProvider>
    </MemoryRouter>,
  );
}

afterEach(() => {
  setRouting.mockClear();
});

describe('TrafficRiver read-only', () => {
  it('should_render_no_sliders_without_editable', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} />);
    expect(screen.queryAllByRole('slider')).toHaveLength(0);
    expect(screen.getByText(/v1 70%/)).toBeInTheDocument();
  });

  it('should_render_no_sliders_for_a_single_version_even_when_editable', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 100, true)]} model="m" editable />);
    expect(screen.queryAllByRole('slider')).toHaveLength(0);
  });

  it('should_render_full_bar_not_dash_for_a_single_active_version', () => {
    // mergeVersionList normalizes "no routing configured" to weight 100, so
    // single-version models must show a river, not a bare placeholder dash.
    renderRiver(<TrafficRiver versions={[version('1', 100, true)]} />);
    expect(screen.queryByText('-')).not.toBeInTheDocument();
    expect(screen.getByText(/1 100%/)).toBeInTheDocument();
  });

  it('should_render_dash_only_when_no_version_carries_weight', () => {
    renderRiver(<TrafficRiver versions={[version('1', 0)]} />);
    expect(screen.getByText('-')).toBeInTheDocument();
  });
});

describe('TrafficRiver editable', () => {
  it('should_render_one_handle_per_boundary', () => {
    renderRiver(
      <TrafficRiver versions={[version('v1', 50, true), version('v2', 30), version('v3', 20)]} model="m" editable />,
    );
    expect(screen.getAllByRole('slider')).toHaveLength(2);
  });

  it('should_shift_one_percent_between_adjacent_versions_on_arrow_key', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    const handle = screen.getAllByRole('slider')[0];
    fireEvent.keyDown(handle, { key: 'ArrowRight' });
    expect(screen.getByText(/v1 71%/)).toBeInTheDocument();
    expect(screen.getByText(/v2 29%/)).toBeInTheDocument();
    expect(handle).toHaveAttribute('aria-valuenow', '71');
  });

  it('should_shift_ten_percent_on_shift_arrow', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    fireEvent.keyDown(screen.getAllByRole('slider')[0], { key: 'ArrowRight', shiftKey: true });
    expect(screen.getByText(/v1 80%/)).toBeInTheDocument();
    expect(screen.getByText(/v2 20%/)).toBeInTheDocument();
  });

  it('should_clamp_at_zero_when_dragging_past_the_edge', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    const handle = screen.getAllByRole('slider')[0];
    for (let i = 0; i < 4; i += 1) fireEvent.keyDown(handle, { key: 'ArrowLeft', shiftKey: true });
    expect(screen.getByText(/v1 30%/)).toBeInTheDocument();
    expect(screen.getByText(/v2 70%/)).toBeInTheDocument();
  });

  it('should_only_touch_the_dragged_boundary_pair', () => {
    renderRiver(
      <TrafficRiver versions={[version('v1', 50, true), version('v2', 30), version('v3', 20)]} model="m" editable />,
    );
    fireEvent.keyDown(screen.getAllByRole('slider')[1], { key: 'ArrowRight', shiftKey: true });
    expect(screen.getByText(/v1 50%/)).toBeInTheDocument();
    expect(screen.getByText(/v2 40%/)).toBeInTheDocument();
    expect(screen.getByText(/v3 10%/)).toBeInTheDocument();
  });

  it('should_show_apply_and_reset_only_after_editing', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    expect(screen.queryByRole('button', { name: 'Apply' })).not.toBeInTheDocument();
    fireEvent.keyDown(screen.getAllByRole('slider')[0], { key: 'ArrowRight' });
    expect(screen.getByRole('button', { name: 'Apply' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Reset' })).toBeInTheDocument();
  });

  it('should_discard_the_draft_on_reset', () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    fireEvent.keyDown(screen.getAllByRole('slider')[0], { key: 'ArrowRight' });
    fireEvent.click(screen.getByRole('button', { name: 'Reset' }));
    expect(screen.getByText(/v1 70%/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Apply' })).not.toBeInTheDocument();
  });

  it('should_discard_the_draft_when_the_version_set_changes_before_apply', () => {
    // Versions refetch every 10s; a draft built by position must never be
    // applied to a changed version set (weights would land on wrong versions).
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const tree = (ui: React.ReactElement) => (
      <MemoryRouter initialEntries={['/?i=prod']}>
        <QueryClientProvider client={queryClient}>
          <AntdApp>
            <InstanceProvider>{ui}</InstanceProvider>
          </AntdApp>
        </QueryClientProvider>
      </MemoryRouter>
    );
    const utils = render(
      tree(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />),
    );
    fireEvent.keyDown(screen.getAllByRole('slider')[0], { key: 'ArrowRight' });
    expect(screen.getByText(/v1 71%/)).toBeInTheDocument();
    utils.rerender(
      tree(<TrafficRiver versions={[version('v1', 70, true), version('v3', 30)]} model="m" editable />),
    );
    expect(screen.getByText(/v1 70%/)).toBeInTheDocument();
    expect(screen.getByText(/v3 30%/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Apply' })).not.toBeInTheDocument();
  });

  it('should_apply_draft_weights_via_set_routing_after_confirm', async () => {
    renderRiver(<TrafficRiver versions={[version('v1', 70, true), version('v2', 30)]} model="m" editable />);
    fireEvent.keyDown(screen.getAllByRole('slider')[0], { key: 'ArrowRight' });
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }));
    // Confirm modal with the before/after diff.
    const dialog = await screen.findByRole('dialog');
    expect(dialog).toHaveTextContent('v1: 70% → 71%');
    fireEvent.click(within(dialog).getByRole('button', { name: 'Apply' }));
    await waitFor(() => expect(setRouting).toHaveBeenCalledWith('prod', 'm', { v1: 71, v2: 29 }));
    // Draft cleared after a successful apply.
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Apply' })).not.toBeInTheDocument());
    expect(screen.getByText(/v1 70%/)).toBeInTheDocument();
  });
});
