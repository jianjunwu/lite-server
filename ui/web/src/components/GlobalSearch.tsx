import { useMemo, useState } from 'react';
import { AutoComplete, Input } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { useQueries } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { apiFetch } from '../api/client';
import { useInstances } from '../api/hooks';
import type { ModelList, RepoIndexResponse } from '../api/types';
import { useNeutrals } from '../context/ThemeModeContext';
import { TYPE } from '../theme';

interface SearchHit {
  instanceId: string;
  instanceName: string;
  model: string;
}

/** Header global search: find a model across every configured instance. */
export function GlobalSearch() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const neutrals = useNeutrals();
  const instancesQuery = useInstances();
  const instances = useMemo(() => instancesQuery.data?.instances ?? [], [instancesQuery.data]);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');

  // Fetch model lists lazily — only while the search box is active. The repo
  // index complements the loaded-only list so unloaded models stay findable.
  const modelQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'models'],
      queryFn: () => apiFetch<ModelList>(i.id, '/v2/models'),
      enabled: open,
      staleTime: 15_000,
      retry: 0,
    })),
  });
  const repoQueries = useQueries({
    queries: instances.map((i) => ({
      queryKey: [i.id, 'repo-index'],
      queryFn: () => apiFetch<RepoIndexResponse>(i.id, '/v2/repository/index', { method: 'POST' }),
      enabled: open,
      staleTime: 15_000,
      retry: 0,
    })),
  });

  const hits = useMemo(() => {
    const q = query.trim().toLowerCase();
    const all: SearchHit[] = [];
    instances.forEach((inst, idx) => {
      const seen = new Set<string>();
      const push = (name: string) => {
        if (seen.has(name)) return;
        seen.add(name);
        if (q && !name.toLowerCase().includes(q)) return;
        all.push({ instanceId: inst.id, instanceName: inst.name, model: name });
      };
      for (const m of modelQueries[idx]?.data?.models ?? []) push(m.name);
      for (const e of repoQueries[idx]?.data?.models ?? []) push(e.name);
    });
    return all.slice(0, 20);
  }, [instances, modelQueries, repoQueries, query]);

  return (
    <AutoComplete
      style={{ minWidth: 260 }}
      value={query}
      onChange={setQuery}
      onDropdownVisibleChange={setOpen}
      onSelect={(value: string) => {
        const hit = hits.find((h) => `${h.instanceId}/${h.model}` === value);
        if (hit) {
          navigate(`/models/${encodeURIComponent(hit.model)}?i=${encodeURIComponent(hit.instanceId)}`);
          setQuery('');
        }
      }}
      options={hits.map((h) => ({
        value: `${h.instanceId}/${h.model}`,
        label: (
          <span style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
            <span>{h.model}</span>
            <span style={{ color: neutrals.textMuted, fontSize: TYPE.eyebrow }}>{h.instanceName}</span>
          </span>
        ),
      }))}
    >
      <Input
        prefix={<SearchOutlined style={{ color: neutrals.textMuted }} />}
        placeholder={t('search.placeholder')}
        allowClear
        size="small"
      />
    </AutoComplete>
  );
}
