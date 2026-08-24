import { Navigate, Route, Routes } from 'react-router-dom';
import { AppLayout } from './layout/AppLayout';
import { InstanceProvider } from './context/InstanceContext';
import { OverviewPage } from './pages/OverviewPage';
import { ModelsPage } from './pages/ModelsPage';
import { ModelDetailPage } from './pages/ModelDetailPage';
import { VersionDetailPage } from './pages/VersionDetailPage';
import { MetricsPage } from './pages/MetricsPage';
import { AlertsPage } from './pages/AlertsPage';
import { PlaygroundPage } from './pages/PlaygroundPage';
import { SettingsPage } from './pages/SettingsPage';

export function App() {
  return (
    <InstanceProvider>
      <Routes>
        <Route element={<AppLayout />}>
          <Route index element={<Navigate to="/overview" replace />} />
          <Route path="/overview" element={<OverviewPage />} />
          <Route path="/models" element={<ModelsPage />} />
          <Route path="/models/:name" element={<ModelDetailPage />} />
          <Route path="/models/:name/versions/:version" element={<VersionDetailPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/alerts" element={<AlertsPage />} />
          <Route path="/playground" element={<PlaygroundPage />} />
          <Route path="/settings" element={<SettingsPage />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Route>
      </Routes>
    </InstanceProvider>
  );
}
