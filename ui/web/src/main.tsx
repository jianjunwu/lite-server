import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { App as AntdApp, ConfigProvider } from 'antd';
import { App } from './App';
import { themeConfig } from './theme';
import { AdminKeyProvider } from './context/AdminKeyContext';
import { TaskProvider } from './context/TaskContext';
import './i18n';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 2_000,
    },
  },
});

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ConfigProvider theme={themeConfig}>
        <AntdApp>
          <TaskProvider>
            <AdminKeyProvider>
              <BrowserRouter>
                <App />
              </BrowserRouter>
            </AdminKeyProvider>
          </TaskProvider>
        </AntdApp>
      </ConfigProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
