//! Regression tests for bugs found in the 2026-10-15 bug hunt.
//!
//! Every test here is written against the *intended* behaviour and currently
//! FAILS, demonstrating a real defect. Each one names the defect it pins.

import { afterEach, describe, expect, it, vi } from 'vitest';

import { mergeModelList } from '../api/merge';
import { inferUnary } from '../api/playground';
import { ApiError } from '../api/client';
import { isVersionExistsConflict } from '../api/upload';
import { statusKind } from '../components/StatusBadge';

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('upload conflict detection', () => {
  it('should_recognise_the_version_exists_conflict_in_the_canonical_envelope', () => {
    // The instance renders every error through the OpenAI-shaped envelope:
    //   {"error": {type, message, code, param}}
    // (src/protocol/openai.rs::render_body), and the overwrite guard emits
    // exactly "version v1 of model m already exists; pass ?force=true to
    // overwrite" (src/http/handlers/files.rs).
    //
    // `isVersionExistsConflict` reads a *flat* `body.code`, so against the
    // real wire shape it always returns false: uploading over an existing
    // version reports a hard failure and the "Overwrite?" confirm + the
    // `force=true` retry never run, even though both are wired up.
    const conflict = new ApiError(
      409,
      'req-1',
      {
        error: {
          type: 'conflict_error',
          code: 'conflict',
          message: 'version 1 of model m already exists; pass ?force=true to overwrite',
          param: null,
        },
      },
      'conflict',
    );

    expect(isVersionExistsConflict(conflict)).toBe(true);
  });
});

describe('model status mapping', () => {
  it('should_map_warming_up_to_a_non_ready_kind', () => {
    // The server serialises the warming-up phase as `warming_up`
    // (VersionStatus::WarmingUp, snake_case): worker handshaked but NOT
    // serving. `statusKind` has no case for it, so it falls through to
    // 'offline' while `mergeModelList` promotes a model with no
    // error/loading versions to 'ready' — the model list shows a green
    // "Ready" badge for a version that explicitly is not serving.
    expect(statusKind('warming_up')).not.toBe('offline');
    expect(statusKind('warming_up')).toBe('loading');
  });

  it('should_not_report_a_warming_up_model_as_ready_in_the_list', () => {
    const loaded = [
      { name: 'm', version: '1', status: 'warming_up', workers: 1 },
    ] as never;

    const merged = mergeModelList(undefined, loaded);
    expect(merged[0].status).not.toBe('ready');
  });
});

describe('playground error rendering', () => {
  it('should_render_the_instance_error_message_not_object_object', async () => {
    // The instance's error envelope nests the human-readable message under
    // `error.message`. The playground does `String(parsed.error)`, which
    // stringifies the *object*, so every rejected infer/SSE request shows
    // the literal "[object Object]" instead of the server's message.
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: {
              type: 'not_found_error',
              code: 'model_not_found',
              message: 'model m not found',
              param: null,
            },
          }),
          { status: 404, headers: { 'content-type': 'application/json' } },
        ),
      ),
    );

    const err = await inferUnary('dev', 'm', null, '{}').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect((err as Error).message).not.toContain('[object Object]');
    expect((err as Error).message).toContain('model m not found');
  });
});
