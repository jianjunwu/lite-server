import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import '../i18n';
import { VersionsTable } from '../components/VersionsTable';
import type { VersionInfo } from '../api/types';

vi.mock('../components/TrafficRiver', () => ({ TrafficRiver: () => null }));

vi.mock('../context/ThemeModeContext', () => ({
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
}));

const loaded = (loaded_at: number): VersionInfo => ({
  version: '1',
  status: 'ready',
  active: true,
  weight: 100,
  workers: { ready: 1, total: 1 },
  loaded_at,
});

describe('VersionsTable loaded-at column', () => {
  it('should_render_relative_age_with_ago_suffix', () => {
    render(
      <MemoryRouter initialEntries={['/?i=prod']}>
        <VersionsTable model="echo" versions={[loaded(Date.now() / 1000 - 182)]} />
      </MemoryRouter>,
    );
    expect(screen.getByText('3m ago')).toBeInTheDocument();
  });
});
