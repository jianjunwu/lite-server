import { bffFetch } from './client';

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
  totpEnabled?: boolean;
}

export type LoginResult = { user: SessionUser } | { totpRequired: true; challenge: string };

export const authApi = {
  login: (username: string, password: string) =>
    bffFetch<LoginResult>('/api/auth/login', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username, password }),
    }),
  verifyTotp: (challenge: string, code: string) =>
    bffFetch<{ user: SessionUser }>('/api/auth/totp', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ challenge, code }),
    }),
  totpEnroll: () =>
    bffFetch<{ secret: string; otpauthUrl: string }>('/api/auth/totp/enroll', { method: 'POST' }),
  totpConfirm: (code: string) =>
    bffFetch<{ backupCodes: string[] }>('/api/auth/totp/confirm', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ code }),
    }),
  totpDisable: (code: string) =>
    bffFetch<{ ok: boolean }>('/api/auth/totp/disable', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ code }),
    }),
  adminResetTotp: (username: string) =>
    bffFetch<{ ok: boolean }>(`/api/users/${encodeURIComponent(username)}/totp`, { method: 'DELETE' }),
  logout: () => bffFetch<{ ok: boolean }>('/api/auth/logout', { method: 'POST' }),
  me: () => bffFetch<{ user: SessionUser }>('/api/auth/me'),
  changePassword: (currentPassword: string, newPassword: string) =>
    bffFetch<{ user: SessionUser }>('/api/auth/change-password', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ currentPassword, newPassword }),
    }),
  registrationStatus: () => bffFetch<RegistrationStatus>('/api/auth/registration'),
  register: (username: string, password: string, inviteCode?: string) =>
    bffFetch<{ user: SessionUser }>('/api/auth/register', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ username, password, inviteCode }),
    }),
};

export interface RegistrationStatus {
  open: boolean;
  inviteRequired: boolean;
}

export interface InviteInfo {
  code: string;
  role: Role;
  maxUses: number;
  useCount: number;
  expiresAt: string | null;
  createdBy: string;
  createdAt: string;
  revokedAt: string | null;
}

export const invitesApi = {
  list: () => bffFetch<{ invites: InviteInfo[] }>('/api/invites'),
  create: (params: { role: Role; maxUses: number; expiresInHours: number | null }) =>
    bffFetch<{ invite: InviteInfo }>('/api/invites', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(params),
    }),
  revoke: (code: string) =>
    bffFetch<{ ok: boolean }>(`/api/invites/${encodeURIComponent(code)}`, { method: 'DELETE' }),
};

export interface SessionInfo {
  id: string;
  createdAt: string;
  lastSeenAt: string;
  ip: string | null;
  userAgent: string | null;
  current: boolean;
}

export const sessionsApi = {
  listMine: () => bffFetch<{ sessions: SessionInfo[] }>('/api/auth/sessions'),
  revokeMine: (id: string) =>
    bffFetch<{ ok: boolean }>(`/api/auth/sessions/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  listFor: (username: string) =>
    bffFetch<{ sessions: SessionInfo[] }>(`/api/users/${encodeURIComponent(username)}/sessions`),
  revokeFor: (username: string, id: string) =>
    bffFetch<{ ok: boolean }>(
      `/api/users/${encodeURIComponent(username)}/sessions/${encodeURIComponent(id)}`,
      { method: 'DELETE' },
    ),
};

export interface AuditEntry {
  id: number;
  ts: string;
  actor: string | null;
  action: string;
  target: string | null;
  ip: string | null;
  detail: Record<string, unknown> | null;
}

export const auditApi = {
  list: (params: { limit?: number; offset?: number; action?: string } = {}) => {
    const qs = new URLSearchParams();
    if (params.limit) qs.set('limit', String(params.limit));
    if (params.offset) qs.set('offset', String(params.offset));
    if (params.action) qs.set('action', params.action);
    const suffix = qs.size ? `?${qs}` : '';
    return bffFetch<{ entries: AuditEntry[] }>(`/api/audit${suffix}`);
  },
};
