import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { describe, expect, it, vi } from 'vitest';
import '../i18n';
import { PageHeader } from '../components/PageHeader';

vi.mock('../context/ThemeModeContext', () => ({
  useNeutrals: () => new Proxy({}, { get: () => '#888' }),
}));

function LocationProbe() {
  const loc = useLocation();
  return <div data-testid="loc">{loc.pathname + loc.search}</div>;
}

function renderHeader(initial = '/models/echo?i=prod') {
  return render(
    <MemoryRouter initialEntries={[initial]}>
      <Routes>
        <Route
          path="*"
          element={
            <>
              <PageHeader
                title="echo"
                breadcrumb={[{ title: 'Models', href: '/models?i=prod' }, { title: 'echo' }]}
                onBack={() => {}}
              />
              <LocationProbe />
            </>
          }
        />
      </Routes>
    </MemoryRouter>,
  );
}

describe('PageHeader navigation', () => {
  it('should_render_breadcrumb_links', () => {
    renderHeader();
    const link = screen.getByRole('link', { name: 'Models' });
    expect(link.getAttribute('href')).toBe('/models?i=prod');
    // 'echo' appears both in the breadcrumb and as the page heading.
    expect(screen.getByRole('heading', { name: 'echo' })).toBeTruthy();
  });

  it('should_render_back_button_when_onBack_is_given', () => {
    const onBack = vi.fn();
    render(
      <MemoryRouter>
        <PageHeader title="echo" onBack={onBack} />
      </MemoryRouter>,
    );
    fireEvent.click(screen.getByRole('button', { name: /back/i }));
    expect(onBack).toHaveBeenCalledTimes(1);
  });

  it('should_render_plain_title_without_navigation_props', () => {
    render(
      <MemoryRouter>
        <PageHeader title="Models" />
      </MemoryRouter>,
    );
    expect(screen.queryByRole('button', { name: /back/i })).toBeNull();
  });
});
