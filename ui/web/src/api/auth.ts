import { ApiError, bffFetch } from './client';

export type Role = 'viewer' | 'operator' | 'admin';
const ROLE_RANK: Record<Role, number> = { viewer: 0, operator: 1, admin: 2 };

export function roleAtLeast(role: Role, required: Role): boolean {
  return ROLE_RANK[role] >= ROLE_RANK[required];
}

export interface SessionUser {
  username: string;
  role: Role;
  createdAt?: string;
  mustChangePassword: boolean;
}

export const authApi = {
  login: (username: string, password: string) =>
    bffFetch<{ user: SessionUser }>('/api/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username, password }),
    }),
  logout: () => bffFetch<{ ok: boolean }>('/api/auth/logout', { method: 'POST' }),
  me: () => bffFetch<{ user: SessionUser }>('/api/auth/me'),
  changePassword: (currentPassword: string, newPassword: string) =>
    bffFetch<{ user: SessionUser }>('/api/auth/change-password', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ currentPassword, newPassword }),
    }),
};

/** Marker the BFF uses for its own 401s (instance 401s pass through with the
 * instance's own body and never carry this marker). */
export function isBffUnauthorized(err: unknown): boolean {
  return (
    err instanceof ApiError &&
    err.status === 401 &&
    err.body !== null &&
    typeof err.body === 'object' &&
    (err.body as { error?: unknown }).error === 'unauthenticated'
  );
}
