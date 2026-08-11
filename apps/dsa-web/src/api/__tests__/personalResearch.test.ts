import { beforeEach, describe, expect, it, vi } from 'vitest';
import { personalResearchApi } from '../personalResearch';

const { post } = vi.hoisted(() => ({ post: vi.fn() }));

vi.mock('../index', () => ({ default: { post } }));

describe('personalResearchApi', () => {
  beforeEach(() => post.mockReset());

  it('submits the formal durable run with an idempotency header', async () => {
    post.mockResolvedValueOnce({
      data: {
        task_id: 'task-1',
        trace_id: 'trace-1',
        status: 'pending',
        created: true,
        deduplicated: false,
        stock_code: '600519',
        market: 'cn',
        requested_mode: 'deep',
        resolved_mode: 'deep',
        priority: 50,
      },
    });

    const result = await personalResearchApi.createRun({
      stockCode: '600519',
      requestedMode: 'deep',
      notify: true,
      reportLanguage: 'zh-CN',
    }, 'web-personal-fixed-key');

    expect(post).toHaveBeenCalledWith(
      '/api/v1/research/personal/runs',
      {
        stock_code: '600519',
        requested_mode: 'deep',
        notify: true,
        report_language: 'zh-CN',
      },
      { headers: { 'Idempotency-Key': 'web-personal-fixed-key' } },
    );
    expect(result).toEqual(expect.objectContaining({
      taskId: 'task-1',
      stockCode: '600519',
      requestedMode: 'deep',
      resolvedMode: 'deep',
    }));
  });
});
