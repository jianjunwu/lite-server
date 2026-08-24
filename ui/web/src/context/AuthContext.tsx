import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { authApi, roleAtLeast, type LoginResult, type Role, type SessionUser } from '../api/auth';
import { setOnBffUnauthorized } from '../api/client';

interface AuthContextValue {
  user: SessionUser | null;
  /** true until the initial /me probe settles. */
  loading: boolean;
  login: (username: string, password: string) => Promise<LoginResult>;
  verifyTotp: (challenge: string, code: string) => Promise<SessionUser>;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
  can: (required: Role) => boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<SessionUser | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const { user: me } = await authApi.me();
      setUser(me);
    } catch {
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  // Any BFF 401 {error:'bff_unauthenticated'} anywhere drops the session.
  useEffect(() => {
    setOnBffUnauthorized(() => setUser(null));
    return () => setOnBffUnauthorized(null);
  }, []);

  const login = useCallback(async (username: string, password: string) => {
    const result = await authApi.login(username, password);
    // With 2FA the session only exists after verifyTotp completes.
    if ('user' in result) setUser(result.user);
    return result;
  }, []);

  const verifyTotp = useCallback(async (challenge: string, code: string) => {
    const { user: me } = await authApi.verifyTotp(challenge, code);
    setUser(me);
    return me;
  }, []);

  const logout = useCallback(async () => {
    try {
      await authApi.logout();
    } finally {
      setUser(null);
    }
  }, []);

  const can = useCallback((required: Role) => (user ? roleAtLeast(user.role, required) : false), [user]);

  return (
    <AuthContext.Provider value={{ user, loading, login, verifyTotp, logout, refresh, can }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
