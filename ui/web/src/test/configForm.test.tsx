/** Audit repro test (feat/admin-enhancement review): ConfigForm validity map. */
import { fireEvent, render } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ConfigForm } from '../components/config/ConfigForm';

function findTextarea(container: HTMLElement, marker: string): HTMLTextAreaElement {
  const areas = Array.from(container.querySelectorAll('textarea'));
  const hit = areas.find((a) => a.value.includes(marker));
  if (!hit) throw new Error(`textarea containing ${marker} not found`);
  return hit;
}

describe('ConfigForm JSON validity tracking', () => {
  it('clears per-field validity when an edit session ends', () => {
    // Audit UI-M4: `validity` is a useRef Map that is only ever written.
    // An invalid-JSON report (false) survives Cancel → re-Edit; the next
    // validity report aggregates over the stale false and locks the Save
    // button even though every field is currently valid.
    const config = { hooks: { a: 1 }, policies: { b: 2 } };
    const onValidity = vi.fn();
    const props = {
      config,
      hasFile: true,
      onChange: () => {},
      onValidityChange: onValidity,
    };

    const { container, rerender } = render(
      <ConfigForm {...props} editing draft={{ ...config }} />,
    );

    // Session 1: make the `hooks` JSON field invalid.
    fireEvent.change(findTextarea(container, '"a": 1'), { target: { value: '{oops' } });
    expect(onValidity).toHaveBeenLastCalledWith(false);

    // Cancel (editing=false), then start a fresh edit session with a clean
    // draft — the same component instance stays mounted.
    rerender(<ConfigForm {...props} editing={false} />);
    onValidity.mockClear();
    rerender(<ConfigForm {...props} editing draft={{ ...config }} />);

    // Touch a DIFFERENT JSON field with valid content.
    fireEvent.change(findTextarea(container, '"b": 2'), { target: { value: '{"b": 3}' } });

    // Every field is currently valid, so the aggregate must be true — the
    // stale `hooks: false` from session 1 must not lock the form.
    expect(onValidity).toHaveBeenLastCalledWith(true);
  });
});
