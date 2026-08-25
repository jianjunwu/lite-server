import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import {
  App, Button, Card, Col, Collapse, Empty, Input, Popconfirm, Row, Segmented, Select, Space, Table, Tag, Typography,
} from 'antd';
import { DeleteOutlined, PlusOutlined, SendOutlined, StopOutlined, SaveOutlined } from '@ant-design/icons';
import { diffLines } from 'diff';
import { useSearchParams } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useInstance } from '../context/InstanceContext';
import { useModels, useVersions } from '../api/hooks';
import {
  addHistory, deleteTemplate, getHistory, inferUnary, listTemplates, loadHeaders, saveHeaders, saveTemplate,
  streamEvents, subscribeHistory, type HeaderRow, type HistoryEntry,
} from '../api/playground';
import { PageHeader } from '../components/PageHeader';
import { formatMs } from '../components/format';
import { MONO_FONT, STATUS_COLORS, TYPE, dataTextStyle } from '../theme';
import { useNeutrals } from '../context/ThemeModeContext';

// ---- response slot state ----------------------------------------------------

interface SlotState {
  status: 'idle' | 'running' | 'done' | 'error';
  /** unary response text, or null while streaming. */
  text: string | null;
  events: string[];
  durationMs: number | null;
  requestId: string | null;
  /** Response headers, null until the first response arrives. */
  headers: Record<string, string> | null;
  error: string | null;
}

const IDLE_SLOT: SlotState = { status: 'idle', text: null, events: [], durationMs: null, requestId: null, headers: null, error: null };

function ResponseMeta({ slot }: { slot: SlotState }) {
  const { t } = useTranslation();
  if (slot.status === 'idle') return null;
  return (
    <Space size="middle" style={{ marginBottom: 8 }}>
      <Tag
        color={slot.status === 'error' ? STATUS_COLORS.error : slot.status === 'done' ? STATUS_COLORS.ready : STATUS_COLORS.loading}
        style={{ border: 'none', color: '#fff' }}
      >
        {slot.status}
      </Tag>
      {slot.durationMs !== null && <span style={dataTextStyle}>{formatMs(slot.durationMs)}</span>}
      {slot.requestId && (
        <Typography.Text type="secondary" style={{ ...dataTextStyle, fontSize: TYPE.eyebrow }}>
          {t('common.requestId')}: {slot.requestId}
        </Typography.Text>
      )}
    </Space>
  );
}

function SlotView({ slot }: { slot: SlotState }) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [slot.events.length]);

  if (slot.status === 'idle') {
    return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={t('playground.noResponse')} />;
  }
  return (
    <div>
      <ResponseMeta slot={slot} />
      {slot.error && <Typography.Text type="danger">{slot.error}</Typography.Text>}
      {slot.headers !== null && (
        <Collapse
          ghost
          size="small"
          style={{ marginBottom: 8 }}
          items={[
            {
              key: 'response-headers',
              label: t('playground.responseHeaders'),
              children: (
                <pre style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary, margin: 0, maxHeight: 200, overflow: 'auto' }}>
                  {Object.entries(slot.headers).map(([k, v]) => `${k}: ${v}`).join('\n')}
                </pre>
              ),
            },
          ]}
        />
      )}
      {slot.text !== null && (
        <pre style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary, margin: 0, maxHeight: 400, overflow: 'auto' }}>
          {slot.text}
        </pre>
      )}
      {slot.events.length > 0 && (
        <div ref={scrollRef} style={{ maxHeight: 400, overflow: 'auto' }}>
          {slot.events.map((e, i) => (
            <pre key={i} style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary, margin: 0, padding: '2px 0', borderBottom: `1px dashed ${neutrals.border}` }}>
              {e}
            </pre>
          ))}
        </div>
      )}
    </div>
  );
}

function DiffView({ textA, textB }: { textA: string; textB: string }) {
  const { t } = useTranslation();
  const neutrals = useNeutrals();
  const parts = diffLines(textA, textB);
  const identical = parts.every((p) => !p.added && !p.removed);
  if (identical) {
    return <Typography.Text type="secondary">{t('playground.identical')}</Typography.Text>;
  }
  return (
    <pre style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary, margin: 0, maxHeight: 300, overflow: 'auto' }}>
      {parts.map((p, i) => (
        <span
          key={i}
          style={{
            display: 'block',
            background: p.added ? neutrals.diffAddBg : p.removed ? neutrals.diffRemoveBg : 'transparent',
            color: p.added ? STATUS_COLORS.ready : p.removed ? STATUS_COLORS.error : undefined,
          }}
        >
          {p.value}
        </span>
      ))}
    </pre>
  );
}

// ---- headers editor ---------------------------------------------------------

function HeadersEditor({ rows, onChange }: { rows: HeaderRow[]; onChange: (rows: HeaderRow[]) => void }) {
  const { t } = useTranslation();
  const hasAuthorization = rows.some((r) => r.name.trim().toLowerCase() === 'authorization');
  const patch = (i: number, part: Partial<HeaderRow>) =>
    onChange(rows.map((r, j) => (j === i ? { ...r, ...part } : r)));
  return (
    <Space direction="vertical" style={{ width: '100%' }} size="small">
      {rows.map((row, i) => (
        <Space.Compact key={i} style={{ width: '100%' }}>
          <Input
            placeholder={t('playground.headerName')}
            value={row.name}
            style={{ width: '40%' }}
            onChange={(e) => patch(i, { name: e.target.value })}
          />
          <Input
            placeholder={t('playground.headerValue')}
            value={row.value}
            onChange={(e) => patch(i, { value: e.target.value })}
          />
          <Button icon={<DeleteOutlined />} onClick={() => onChange(rows.filter((_, j) => j !== i))} />
        </Space.Compact>
      ))}
      <Button
        type="dashed"
        size="small"
        icon={<PlusOutlined />}
        onClick={() => onChange([...rows, { name: '', value: '' }])}
      >
        {t('playground.addHeader')}
      </Button>
      {hasAuthorization && (
        <Typography.Text type="warning" style={{ fontSize: TYPE.eyebrow }}>
          {t('playground.authorizationHint')}
        </Typography.Text>
      )}
    </Space>
  );
}

// ---- page -------------------------------------------------------------------

export function PlaygroundPage() {
  const { t } = useTranslation();
  const { message } = App.useApp();
  const { instanceId } = useInstance();
  const [searchParams] = useSearchParams();

  const modelsQuery = useModels(instanceId);
  const modelNames = useMemo(
    () => [...new Set((modelsQuery.data?.models ?? []).map((m) => m.name))],
    [modelsQuery.data],
  );
  const [model, setModel] = useState<string | null>(searchParams.get('model'));
  const effectiveModel = model ?? modelNames[0] ?? null;

  const versionsQuery = useVersions(instanceId, effectiveModel ?? '');
  const versionOptions = useMemo(() => {
    const vs = versionsQuery.data;
    const opts = (vs?.versions ?? []).map((v) => ({ value: v.version, label: v.version }));
    // Empty = no version in the URL: the request goes through weighted
    // routing (active version is only the no-weights fallback).
    return [{ value: '', label: t('playground.activeVersion') }, ...opts];
  }, [versionsQuery.data, t]);

  const [versionA, setVersionA] = useState<string>(searchParams.get('version') ?? '');
  const [versionB, setVersionB] = useState<string>('');
  const [compare, setCompare] = useState(false);
  const [protocol, setProtocol] = useState<'unary' | 'stream'>('unary');
  const [body, setBody] = useState('{\n  "input": 21\n}');
  const [templates, setTemplates] = useState(() => (effectiveModel ? listTemplates(effectiveModel) : []));
  const [headerRows, setHeaderRows] = useState<HeaderRow[]>([]);
  const [slotA, setSlotA] = useState<SlotState>(IDLE_SLOT);
  const [slotB, setSlotB] = useState<SlotState>(IDLE_SLOT);
  const aborts = useRef<Array<() => void>>([]);
  const history = useSyncExternalStore(subscribeHistory, getHistory);

  useEffect(() => {
    setTemplates(effectiveModel ? listTemplates(effectiveModel) : []);
  }, [effectiveModel]);

  useEffect(() => {
    setHeaderRows(instanceId && effectiveModel ? loadHeaders(instanceId, effectiveModel) : []);
  }, [instanceId, effectiveModel]);

  const updateHeaders = (rows: HeaderRow[]) => {
    setHeaderRows(rows);
    if (instanceId && effectiveModel) saveHeaders(instanceId, effectiveModel, rows);
  };

  // Abort in-flight requests when the page unmounts; otherwise orphan
  // streams keep generating on the instance after navigation.
  useEffect(() => {
    return () => {
      aborts.current.forEach((fn) => fn());
      aborts.current = [];
    };
  }, []);

  const running = slotA.status === 'running' || slotB.status === 'running';

  const setSlot = (which: 'a' | 'b', patch: Partial<SlotState> | ((cur: SlotState) => SlotState)) => {
    const setter = which === 'a' ? setSlotA : setSlotB;
    setter((cur) => (typeof patch === 'function' ? patch(cur) : { ...cur, ...patch }));
  };

  const stop = () => {
    aborts.current.forEach((fn) => fn());
    aborts.current = [];
    for (const which of ['a', 'b'] as const) {
      setSlot(which, (cur) => (cur.status === 'running' ? { ...cur, status: 'done', error: t('playground.stopped') } : cur));
    }
  };

  const runSlot = (which: 'a' | 'b', version: string | null, requestBody: string) => {
    if (!instanceId || !effectiveModel) return Promise.resolve(false);
    setSlot(which, { ...IDLE_SLOT, status: 'running' });
    if (protocol === 'unary') {
      const controller = new AbortController();
      aborts.current.push(() => controller.abort());
      return inferUnary(instanceId, effectiveModel, version, requestBody, controller.signal, headerRows)
        .then((res) => {
          setSlot(which, { status: 'done', text: res.text, durationMs: res.durationMs, requestId: res.requestId, headers: res.headers });
          return true;
        })
        .catch((err) => {
          // stop() already marked the slot; don't overwrite it with an
          // AbortError message.
          if (controller.signal.aborted) return false;
          setSlot(which, {
            status: 'error',
            error: err instanceof Error ? err.message : String(err),
            requestId: err?.requestId ?? null,
          });
          return false;
        });
    }
    return new Promise<boolean>((resolve) => {
      const abort = streamEvents(instanceId, effectiveModel, version, requestBody, {
        onEvent: (payload) => setSlot(which, (cur) => ({ ...cur, events: [...cur.events, payload] })),
        onDone: (durationMs) => {
          setSlot(which, { status: 'done', durationMs });
          resolve(true);
        },
        onError: (err) => {
          setSlot(which, { status: 'error', error: err.message });
          resolve(false);
        },
        onHeaders: (headers) => setSlot(which, { headers }),
      }, headerRows);
      aborts.current.push(abort);
    });
  };

  const send = async () => {
    if (!instanceId || !effectiveModel) return;
    try {
      JSON.parse(body);
    } catch {
      message.error(t('playground.invalidJson'));
      return;
    }
    aborts.current = [];
    const vA = versionA || null;
    const vB = versionB || null;
    const startedAt = performance.now();
    const results = compare
      ? await Promise.all([runSlot('a', vA, body), runSlot('b', vB, body)])
      : [await runSlot('a', vA, body)];
    addHistory({
      model: effectiveModel,
      versionA: vA,
      versionB: compare ? vB : null,
      mode: protocol,
      body,
      ok: results.every(Boolean),
      durationMs: protocol === 'unary' ? performance.now() - startedAt : null,
    });
  };

  const loadHistory = (entry: HistoryEntry) => {
    setModel(entry.model);
    setVersionA(entry.versionA ?? '');
    setVersionB(entry.versionB ?? '');
    setCompare(entry.versionB !== null);
    setProtocol(entry.mode);
    setBody(entry.body);
  };

  const responseArea = compare ? (
    <Row gutter={16}>
      <Col span={12}>
        <Card size="small" title={`A · ${versionA || t('playground.activeVersion')}`}>
          <SlotView slot={slotA} />
        </Card>
      </Col>
      <Col span={12}>
        <Card size="small" title={`B · ${versionB || t('playground.activeVersion')}`}>
          <SlotView slot={slotB} />
        </Card>
      </Col>
      {slotA.text !== null && slotB.text !== null && (
        <Col span={24} style={{ marginTop: 16 }}>
          <Card size="small" title={t('playground.diff')}>
            <DiffView textA={slotA.text} textB={slotB.text} />
          </Card>
        </Col>
      )}
    </Row>
  ) : (
    <Card size="small" title={t('playground.response')}>
      <SlotView slot={slotA} />
    </Card>
  );

  return (
    <>
      <PageHeader title={t('nav.playground')} subtitle={instanceId} />
      <Row gutter={[16, 16]}>
        <Col xs={24} xl={10}>
          <Card size="small">
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <Select
                style={{ width: '100%' }}
                placeholder={t('metrics.model')}
                value={effectiveModel ?? undefined}
                onChange={(v) => {
                  setModel(v);
                  setVersionA('');
                  setVersionB('');
                }}
                options={modelNames.map((m) => ({ value: m, label: m }))}
                loading={modelsQuery.isLoading}
              />
              <Space wrap>
                <Segmented
                  value={compare ? 'ab' : 'single'}
                  onChange={(v) => setCompare(v === 'ab')}
                  options={[
                    { value: 'single', label: t('playground.single') },
                    { value: 'ab', label: 'A/B' },
                  ]}
                />
                <Segmented
                  value={protocol}
                  onChange={(v) => setProtocol(v as 'unary' | 'stream')}
                  options={[
                    { value: 'unary', label: 'Unary' },
                    { value: 'stream', label: 'SSE' },
                  ]}
                />
              </Space>
              <Space wrap>
                <Select
                  style={{ minWidth: 160 }}
                  value={versionA}
                  onChange={setVersionA}
                  options={versionOptions}
                  disabled={!effectiveModel}
                />
                {compare && (
                  <Select
                    style={{ minWidth: 160 }}
                    value={versionB}
                    onChange={setVersionB}
                    options={versionOptions}
                    disabled={!effectiveModel}
                  />
                )}
              </Space>
              <Space.Compact style={{ width: '100%' }}>
                <Select
                  style={{ flex: 1 }}
                  placeholder={t('playground.templates')}
                  allowClear
                  value={null}
                  onChange={(name: string | null) => {
                    const tpl = templates.find((x) => x.name === name);
                    if (tpl) setBody(tpl.body);
                  }}
                  options={templates.map((x) => ({
                    value: x.name,
                    label: (
                      <span style={{ display: 'flex', justifyContent: 'space-between' }}>
                        {x.name}
                        <Typography.Text
                          type="danger"
                          style={{ fontSize: TYPE.eyebrow }}
                          onClick={(e) => {
                            e.stopPropagation();
                            if (effectiveModel) {
                              deleteTemplate(effectiveModel, x.name);
                              setTemplates(listTemplates(effectiveModel));
                            }
                          }}
                        >
                          ✕
                        </Typography.Text>
                      </span>
                    ),
                  }))}
                />
                <Button
                  icon={<SaveOutlined />}
                  onClick={() => {
                    const name = window.prompt(t('playground.templateName'));
                    if (name && effectiveModel) {
                      saveTemplate(effectiveModel, name, body);
                      setTemplates(listTemplates(effectiveModel));
                      message.success(t('playground.templateSaved'));
                    }
                  }}
                >
                  {t('playground.saveTemplate')}
                </Button>
              </Space.Compact>
              <Input.TextArea
                value={body}
                onChange={(e) => setBody(e.target.value)}
                rows={9}
                style={{ fontFamily: MONO_FONT, fontSize: TYPE.secondary }}
                spellCheck={false}
              />
              <Collapse
                ghost
                size="small"
                items={[
                  {
                    key: 'headers',
                    label: headerRows.length > 0 ? `${t('playground.headers')} (${headerRows.length})` : t('playground.headers'),
                    children: <HeadersEditor rows={headerRows} onChange={updateHeaders} />,
                  },
                ]}
              />
              <Space>
                {running ? (
                  <Button danger icon={<StopOutlined />} onClick={stop}>
                    {t('playground.stop')}
                  </Button>
                ) : (
                  <Button type="primary" icon={<SendOutlined />} onClick={send} disabled={!effectiveModel}>
                    {t('playground.send')}
                  </Button>
                )}
              </Space>
            </Space>
          </Card>
        </Col>
        <Col xs={24} xl={14}>{responseArea}</Col>
      </Row>

      <Card size="small" title={t('playground.history')} style={{ marginTop: 16 }}>
        <Table<HistoryEntry>
          size="small"
          rowKey="id"
          dataSource={history}
          pagination={{ pageSize: 10, hideOnSinglePage: true }}
          locale={{ emptyText: t('playground.emptyHistory') }}
          columns={[
            {
              title: t('alerts.time'),
              dataIndex: 'at',
              width: 100,
              render: (at: number) => (
                <span style={dataTextStyle}>{new Date(at).toLocaleTimeString()}</span>
              ),
            },
            { title: t('metrics.model'), dataIndex: 'model', render: (m: string) => <span style={dataTextStyle}>{m}</span> },
            {
              title: t('common.version'),
              width: 150,
              render: (_: unknown, h: HistoryEntry) => (
                <span style={dataTextStyle}>
                  {h.versionA || 'auto'}
                  {h.versionB !== null ? ` ↔ ${h.versionB || 'auto'}` : ''}
                </span>
              ),
            },
            { title: 'mode', dataIndex: 'mode', width: 80 },
            {
              title: t('common.status'),
              width: 90,
              render: (_: unknown, h: HistoryEntry) => (
                <Tag color={h.ok ? STATUS_COLORS.ready : STATUS_COLORS.error} style={{ border: 'none', color: '#fff' }}>
                  {h.ok ? 'ok' : 'fail'}
                </Tag>
              ),
            },
            {
              title: '',
              width: 80,
              render: (_: unknown, h: HistoryEntry) => (
                <Button type="link" size="small" onClick={() => loadHistory(h)}>
                  {t('playground.load')}
                </Button>
              ),
            },
          ]}
        />
      </Card>
    </>
  );
}
