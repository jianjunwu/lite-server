import { lazy, Suspense } from 'react';
import { Navigate, Route, Routes, useLocation } from 'react-router-dom';
import { Spin } from 'antd';
import { AppLayout } from './layout/AppLayout';
import { InstanceProvider } from './context/InstanceContext';
import { RequireAuth } from './components/RequireAuth';
import { LoginPage } from './pages/LoginPage';
import { RegisterPage } from './pages/RegisterPage';

// Route-level code splitting: each page (and its share of echarts/antd)
// loads on demand instead of inflating the entry bundle.
const OverviewPage = lazy(() => import('./pages/OverviewPage').then((m) => ({ default: m.OverviewPage })));
const InstancesPage = lazy(() => import('./pages/InstancesPage').then((m) => ({ default: m.InstancesPage })));
const ModelsPage = lazy(() => import('./pages/ModelsPage').then((m) => ({ default: m.ModelsPage })));
const ModelDetailPage = lazy(() => import('./pages/ModelDetailPage').then((m) => ({ default: m.ModelDetailPage })));
const VersionDetailPage = lazy(() => import('./pages/VersionDetailPage').then((m) => ({ default: m.VersionDetailPage })));
const AlertsPage = lazy(() => import('./pages/AlertsPage').then((m) => ({ default: m.AlertsPage })));
const PlaygroundPage = lazy(() => import('./pages/PlaygroundPage').then((m) => ({ default: m.PlaygroundPage })));
const SettingsPage = lazy(() => import('./pages/SettingsPage').then((m) => ({ default: m.SettingsPage })));
const InstanceDetailPage = lazy(() => import('./pages/InstanceDetailPage').then((m) => ({ default: m.InstanceDetailPage })));

/** /metrics is retired (hierarchy plan §6): the same capabilities live on
 * the model detail page. Preserve the ?i= pin so "I want model metrics"
 * lands on the right instance's model list. */
function MetricsRedirect() {
  const location = useLocation();
  return <Navigate to={`/models${location.search}`} replace />;
}

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
          <Route path="/instances" element={<Lazy><InstancesPage /></Lazy>} />
          <Route path="/models" element={<Lazy><ModelsPage /></Lazy>} />
          <Route path="/models/:name" element={<Lazy><ModelDetailPage /></Lazy>} />
          <Route path="/models/:name/versions/:version" element={<Lazy><VersionDetailPage /></Lazy>} />
          <Route path="/metrics" element={<MetricsRedirect />} />
          <Route path="/alerts" element={<Lazy><AlertsPage /></Lazy>} />
          <Route path="/playground" element={<Lazy><PlaygroundPage /></Lazy>} />
          <Route path="/settings" element={<Lazy><SettingsPage /></Lazy>} />
          <Route path="/instances/:id" element={<Lazy><InstanceDetailPage /></Lazy>} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Route>
      </Routes>
    </InstanceProvider>
  );
}
