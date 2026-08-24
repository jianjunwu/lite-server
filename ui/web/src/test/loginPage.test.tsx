import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { App as AntdApp } from 'antd';
import { MemoryRouter } from 'react-router-dom';
import { afterEach, describe, expect, it, vi } from 'vitest';
import '../i18n';
import { ApiError } from '../api/client';
import { LoginPage } from '../pages/LoginPage';

const login = vi.fn();
const verifyTotp = vi.fn();
vi.mock('../context/AuthContext', async (importOriginal) => {
  const mod = await importOriginal<typeof import('../context/AuthContext')>();
  return {
    ...mod,
    useAuth: () => ({ user: null, loading: false, login, verifyTotp }),
  };
});

function renderLogin() {
  return render(
    <MemoryRouter>
      <AntdApp>
        <LoginPage />
      </AntdApp>
    </MemoryRouter>,
  );
}

afterEach(() => {
  login.mockReset();
  verifyTotp.mockReset();
});

describe('LoginPage two-factor flow', () => {
  it('should_ask_for_the_second_factor_when_login_returns_a_challenge', async () => {
    login.mockResolvedValue({ totpRequired: true, challenge: 'chal-1' });
    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'Admin-pass-1234' } });
    fireEvent.click(screen.getByRole('button', { name: 'Log in' }));
    expect(await screen.findByLabelText('Authenticator code')).toBeTruthy();
    expect(login).toHaveBeenCalledWith('admin', 'Admin-pass-1234');
  });

  it('should_submit_the_totp_code_with_the_challenge', async () => {
    login.mockResolvedValue({ totpRequired: true, challenge: 'chal-1' });
    verifyTotp.mockResolvedValue({ username: 'admin', role: 'admin', mustChangePassword: false });
    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'Admin-pass-1234' } });
    fireEvent.click(screen.getByRole('button', { name: 'Log in' }));
    fireEvent.change(await screen.findByLabelText('Authenticator code'), { target: { value: '123456' } });
    fireEvent.click(screen.getByRole('button', { name: 'Verify' }));
    await waitFor(() => expect(verifyTotp).toHaveBeenCalledWith('chal-1', '123456'));
  });

  it('should_show_an_error_when_the_totp_code_is_rejected', async () => {
    login.mockResolvedValue({ totpRequired: true, challenge: 'chal-1' });
    verifyTotp.mockRejectedValue(new Error('HTTP 401'));
    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'Admin-pass-1234' } });
    fireEvent.click(screen.getByRole('button', { name: 'Log in' }));
    fireEvent.change(await screen.findByLabelText('Authenticator code'), { target: { value: '000000' } });
    fireEvent.click(screen.getByRole('button', { name: 'Verify' }));
    expect(await screen.findByText('Invalid or expired code — try again')).toBeTruthy();
  });

  it('should_show_the_lockout_message_with_retry_after_on_423', async () => {
    login.mockRejectedValue(
      new ApiError(423, null, { error: 'account_locked', retryAfterSec: 600 }, 'account_locked'),
    );
    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'Admin-pass-1234' } });
    fireEvent.click(screen.getByRole('button', { name: 'Log in' }));
    expect(await screen.findByText('Account locked — try again in 600s')).toBeTruthy();
  });

  it('should_show_the_throttle_message_on_429', async () => {
    login.mockRejectedValue(new ApiError(429, null, { error: 'too_many_attempts' }, 'too_many_attempts'));
    renderLogin();
    fireEvent.change(screen.getByLabelText('Username'), { target: { value: 'admin' } });
    fireEvent.change(screen.getByLabelText('Password'), { target: { value: 'Admin-pass-1234' } });
    fireEvent.click(screen.getByRole('button', { name: 'Log in' }));
    expect(await screen.findByText('Too many attempts — please wait and try again later')).toBeTruthy();
  });
});
