import { afterEach, describe, expect, it } from 'vitest';
import { SseParser, deleteTemplate, listTemplates, saveTemplate } from '../api/playground';

describe('SseParser', () => {
  it('should_parse_complete_frames', () => {
    const p = new SseParser();
    expect(p.push('data: chunk-0\n\ndata: chunk-1\n\n')).toEqual(['chunk-0', 'chunk-1']);
  });

  it('should_buffer_partial_frames_across_pushes', () => {
    const p = new SseParser();
    expect(p.push('data: chu')).toEqual([]);
    expect(p.push('nk-0\n\nda')).toEqual(['chunk-0']);
    expect(p.push('ta: x\n\n')).toEqual(['x']);
  });

  it('should_strip_single_leading_space_after_data_colon', () => {
    const p = new SseParser();
    expect(p.push('data: {"a":1}\n\n')).toEqual(['{"a":1}']);
  });

  it('should_join_multiline_data_fields', () => {
    const p = new SseParser();
    expect(p.push('data: line1\ndata: line2\n\n')).toEqual(['line1\nline2']);
  });

  it('should_ignore_non_data_lines', () => {
    const p = new SseParser();
    expect(p.push('event: message\ndata: payload\n\n')).toEqual(['payload']);
  });

  it('should_emit_done_marker_as_regular_payload', () => {
    const p = new SseParser();
    expect(p.push('data: [DONE]\n\n')).toEqual(['[DONE]']);
  });
});

describe('request templates', () => {
  afterEach(() => localStorage.clear());

  it('should_save_and_list_templates_per_model', () => {
    saveTemplate('echo', 'basic', '{"input": 1}');
    saveTemplate('echo', 'big', '{"input": 99}');
    saveTemplate('other', 'x', '{}');
    expect(listTemplates('echo').map((t) => t.name)).toEqual(['basic', 'big']);
    expect(listTemplates('other')).toHaveLength(1);
  });

  it('should_overwrite_template_with_same_name', () => {
    saveTemplate('echo', 'basic', '{"input": 1}');
    saveTemplate('echo', 'basic', '{"input": 2}');
    const list = listTemplates('echo');
    expect(list).toHaveLength(1);
    expect(list[0].body).toBe('{"input": 2}');
  });

  it('should_delete_template', () => {
    saveTemplate('echo', 'basic', '{}');
    deleteTemplate('echo', 'basic');
    expect(listTemplates('echo')).toHaveLength(0);
  });

  it('should_return_empty_for_corrupt_storage', () => {
    localStorage.setItem('lite-ui-tpl:echo', 'not-json');
    expect(listTemplates('echo')).toEqual([]);
  });
});
