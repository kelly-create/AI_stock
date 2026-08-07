import { beforeEach, describe, expect, it, vi } from 'vitest';
import { analysisApi } from '../analysis';

const post = vi.hoisted(() => vi.fn());

vi.mock('../index', () => ({
  default: {
    get: vi.fn(),
    post,
  },
}));

describe('analysisApi.triggerMarketReview', () => {
  beforeEach(() => {
    post.mockReset();
    post.mockResolvedValue({
      status: 202,
      data: {
        status: 'accepted',
        message: 'accepted',
        send_notification: true,
        region: 'cn,us',
        task_id: 'market-task-1',
      },
    });
  });

  it('serializes selected markets to a comma-separated request string', async () => {
    const result = await analysisApi.triggerMarketReview({
      sendNotification: false,
      regions: ['cn', 'us'],
    });

    expect(post).toHaveBeenCalledWith(
      '/api/v1/analysis/market-review',
      {
        send_notification: false,
        report_language: undefined,
        region: 'cn,us',
      },
      expect.any(Object),
    );
    expect(result.region).toBe('cn,us');
  });

  it('omits region when the caller inherits the server default', async () => {
    await analysisApi.triggerMarketReview({ sendNotification: true });

    expect(post).toHaveBeenCalledWith(
      '/api/v1/analysis/market-review',
      {
        send_notification: true,
        report_language: undefined,
      },
      expect.any(Object),
    );
  });
});

describe('analysisApi.cancelTask', () => {
  it('encodes the task id and converts the durable response', async () => {
    post.mockResolvedValueOnce({
      status: 200,
      data: {
        task_id: 'job/with space',
        status: 'cancel_requested',
        message: 'queued',
      },
    });

    const result = await analysisApi.cancelTask('job/with space');

    expect(post).toHaveBeenCalledWith(
      '/api/v1/analysis/tasks/job%2Fwith%20space/cancel',
    );
    expect(result).toEqual({
      taskId: 'job/with space',
      status: 'cancel_requested',
      message: 'queued',
    });
  });
});
