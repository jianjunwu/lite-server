import { lazy, Suspense } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';
import { Spin } from 'antd';
import { AppLayout } from './layout/AppLayout';
import { InstanceProvider } from './context/InstanceContext';
import { RequireAuth } from './components/RequireAuth';
import { LoginPage } from './pages/LoginPage';
import { RegisterPage } from './pages/RegisterPage';

// Route-level code splitting: each page (and its share of echarts/antd)
// loads on demand instead of inflating the entry bundle.
const OverviewPage = lazy(() => import('./pages/OverviewPage').then((m) => ({ default: m.OverviewPage })));
const ModelsPage = lazy(() => import('./pages/ModelsPage').then((m) => ({ default: m.ModelsPage })));
const ModelDetailPage = lazy(() => import('./pages/ModelDetailPage').then((m) => ({ default: m.ModelDetailPage })));
const VersionDetailPage = lazy(() => import('./pages/VersionDetailPage').then((m) => ({ default: m.VersionDetailPage })));
const MetricsPage = lazy(() => import('./pages/MetricsPage').then((m) => ({ default: m.MetricsPage })));
const AlertsPage = lazy(() => import('./pages/AlertsPage').then((m) => ({ default: m.AlertsPage })));
const PlaygroundPage = lazy(() => import('./pages/PlaygroundPage').then((m) => ({ default: m.PlaygroundPage })));
const SettingsPage = lazy(() => import('./pages/SettingsPage').then((m) => ({ default: m.SettingsPage })));

function Lazy({ children }: { children: React.ReactNode }) {
  return (
    <Suspense
      fallback={
        <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 80 }}>
          <Spin size="large" />
        </div>
      }
    >
      {children}
    </Suspense>
  );
}

export function App() {
  return (
    <InstanceProvider>
      <Routes>
        <Route path="/login" element={<LoginPage />} />
        <Route path="/register" element={<RegisterPage />} />
        <Route
          element={
            <RequireAuth>
              <AppLayout />
            </RequireAuth>
          }
        >
          <Route index element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<Lazy><OverviewPage /></Lazy>} />
          <Route path="/models" element={<Lazy><ModelsPage /></Lazy>} />
          <Route path="/models/:name" element={<Lazy><ModelDetailPage /></Lazy>} />
          <Route path="/models/:name/versions/:version" element={<Lazy><VersionDetailPage /></Lazy>} />
          <Route path="/metrics" element={<Lazy><MetricsPage /></Lazy>} />
          <Route path="/alerts" element={<Lazy><AlertsPage /></Lazy>} />
          <Route path="/playground" element={<Lazy><PlaygroundPage /></Lazy>} />
          <Route path="/settings" element={<Lazy><SettingsPage /></Lazy>} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Route>
      </Routes>
    </InstanceProvider>
  );
}
