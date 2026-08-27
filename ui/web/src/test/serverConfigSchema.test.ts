import { describe, expect, it } from 'vitest';
import { groupServerConfig, sourceTagColor } from '../components/config/serverConfigSchema';

describe('groupServerConfig', () => {
  const sources = {
    'server.http_port': 'cli',
    'server.host': 'default',
    'metrics.timeline_max_points': 'file',
  } as const;

  it('should group leaf rows under their top-level section with source labels', () => {
    const groups = groupServerConfig(
      {
        server: { http_port: 8000, host: '0.0.0.0' },
        metrics: { timeline_max_points: 1440 },
      },
      { ...sources },
      [],
    );
    expect(groups.map((g) => g.key)).toEqual(['server', 'metrics']);
    expect(groups[0].rows).toEqual([
      { path: 'server.http_port', value: 8000, source: 'cli', redacted: false },
      { path: 'server.host', value: '0.0.0.0', source: 'default', redacted: false },
    ]);
    expect(groups[1].rows[0].source).toBe('file');
  });

  it('should preserve SERVER_CONFIG_SECTIONS ordering regardless of input order', () => {
    const groups = groupServerConfig(
      { metrics: { enabled: true }, server: { http_port: 8000 } },
      {},
      [],
    );
    expect(groups.map((g) => g.key)).toEqual(['server', 'metrics']);
  });

  it('should bucket unknown top-level sections into a trailing advanced group', () => {
    const groups = groupServerConfig(
      { server: { http_port: 8000 }, future_section: { knob: 1 } },
      {},
      [],
    );
    expect(groups.map((g) => g.key)).toEqual(['server', 'advanced']);
    expect(groups[1].rows).toEqual([
      { path: 'future_section.knob', value: 1, source: 'default', redacted: false },
    ]);
  });

  it('should treat arrays and empty objects as leaf values', () => {
    const groups = groupServerConfig(
      { server: { trusted_proxies: ['10.0.0.0/8'] }, telemetry: { resource_attributes: {} } },
      {},
      [],
    );
    const server = groups.find((g) => g.key === 'server')!;
    expect(server.rows).toEqual([
      { path: 'server.trusted_proxies', value: ['10.0.0.0/8'], source: 'default', redacted: false },
    ]);
    const telemetry = groups.find((g) => g.key === 'telemetry')!;
    expect(telemetry.rows[0].path).toBe('telemetry.resource_attributes');
  });

  it('should flag rows at or under a redacted path', () => {
    const groups = groupServerConfig(
      {
        access_control: { admin: { http: { value: '***', key: 'x-api-key' } } },
        telemetry: { otlp_headers: '***' },
      },
      {},
      ['access_control.admin.http.value', 'telemetry.otlp_headers'],
    );
    const ac = groups.find((g) => g.key === 'access_control')!;
    expect(ac.rows.find((r) => r.path === 'access_control.admin.http.value')!.redacted).toBe(true);
    expect(ac.rows.find((r) => r.path === 'access_control.admin.http.key')!.redacted).toBe(false);
    const telemetry = groups.find((g) => g.key === 'telemetry')!;
    expect(telemetry.rows[0].redacted).toBe(true);
  });
});

describe('sourceTagColor', () => {
  it('should map sources to their badge colors', () => {
    expect(sourceTagColor('cli')).toBe('warning');
    expect(sourceTagColor('file')).toBe('processing');
    expect(sourceTagColor('default')).toBe('default');
  });
});
