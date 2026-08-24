import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, getAdminKey } from '../api/client';
import { setKeyRequester, validateWeights, withAdminKeyRetry } from '../api/mutations';

afterEach(() => {
  setKeyRequester(null);
  sessionStorage.clear();
});

describe('validateWeights', () => {
  it('should_accept_weights_summing_to_100', () => {
    expect(validateWeights({ v1: 90, v2: 10 })).toEqual({ ok: true, sum: 100 });
  });
  it('should_reject_sum_other_than_100', () => {
    expect(validateWeights({ v1: 90, v2: 5 })).toEqual({ ok: false, sum: 95 });
  });
  it('should_reject_negative_or_fractional_weights', () => {
    expect(validateWeights({ v1: 110, v2: -10 }).ok).toBe(false);
    expect(validateWeights({ v1: 99.5, v2: 0.5 }).ok).toBe(false);
  });
  it('should_accept_single_version_at_100', () => {
    expect(validateWeights({ v1: 100 }).ok).toBe(true);
  });
});

describe('withAdminKeyRetry', () => {
  it('should_return_result_without_prompt_when_first_attempt_succeeds', async () => {
    const requester = vi.fn();
    setKeyRequester(requester);
    const result = await withAdminKeyRetry('prod', async () => 'ok');
    expect(result).toBe('ok');
    expect(requester).not.toHaveBeenCalled();
  });

  it('should_prompt_for_key_and_retry_once_on_401', async () => {
    setKeyRequester(async () => 'typed-key');
    let calls = 0;
    const result = await withAdminKeyRetry('prod', async () => {
      calls += 1;
      if (calls === 1) throw new ApiError(401, null, null, 'unauthorized');
      return 'retried';
    });
    expect(result).toBe('retried');
    expect(calls).toBe(2);
    expect(getAdminKey('prod')).toBe('typed-key');
  });

  it('should_rethrow_original_401_when_user_cancels_prompt', async () => {
    setKeyRequester(async () => null);
    const err = await withAdminKeyRetry('prod', async () => {
      throw new ApiError(401, null, null, 'unauthorized');
    }).catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(getAdminKey('prod')).toBeNull();
  });

  it('should_rethrow_immediately_when_no_requester_registered', async () => {
    const err = await withAdminKeyRetry('prod', async () => {
      throw new ApiError(401, null, null, 'unauthorized');
    }).catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
  });

  it('should_not_retry_non_401_errors', async () => {
    const requester = vi.fn();
    setKeyRequester(requester);
    const err = await withAdminKeyRetry('prod', async () => {
      throw new ApiError(500, null, null, 'boom');
    }).catch((e) => e);
    expect((err as ApiError).status).toBe(500);
    expect(requester).not.toHaveBeenCalled();
  });

  it('should_not_prompt_when_401_marks_bff_session_expiry', async () => {
    // BFF-side 401s ({error:'unauthenticated'}) mean the login session died;
    // the auth flow handles those. Prompting for an instance key on top of
    // the login redirect is wrong, and the retry would fail anyway.
    const requester = vi.fn();
    setKeyRequester(requester);
    const err = await withAdminKeyRetry('prod', async () => {
      throw new ApiError(401, null, { error: 'unauthenticated' }, 'unauthenticated');
    }).catch((e) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(requester).not.toHaveBeenCalled();
  });
});
