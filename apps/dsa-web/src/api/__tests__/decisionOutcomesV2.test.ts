import { beforeEach, describe, expect, it, vi } from 'vitest';
import apiClient from '../index';
import { decisionOutcomesV2Api } from '../decisionOutcomesV2';

vi.mock('../index', () => ({
  default: { get: vi.fn() },
}));

const collection = {
  contract: 'decision-outcome-v2-collection',
  version: 'v1',
  total: 1,
  page: 1,
  page_size: 20,
  items: [{
    id: 1,
    signal_id: 42,
    outcome_contract: 'decision-outcome-v2',
    horizon: '5d',
    dataset_hashes: ['a'.repeat(64)],
    csi300: { status: 'unavailable' },
    sw1: { status: 'unavailable' },
  }],
};

const stats = {
  contract: 'decision-outcome-v2-stats',
  version: 'v1',
  engine_version: 'personal-research-outcome-v2',
  horizons: ['5d', '10d', '20d'],
  bucket_dimensions: ['engine', 'horizon', 'profile', 'final_action_family'],
  minimum_completed_sample_size: 30,
  calibration_bin_count: 5,
  buckets: [{
    dimensions: {
      engine: 'personal-research-outcome-v2',
      horizon: '5d',
      profile: 'balanced',
      final_action_family: 'long',
    },
    bins: [],
    benchmarks: { csi300: {}, sw1: {} },
  }],
};

describe('decisionOutcomesV2Api', () => {
  beforeEach(() => vi.clearAllMocks());

  it('keeps the v2 collection contract independent and maps snake case', async () => {
    vi.mocked(apiClient.get).mockResolvedValueOnce({ data: collection });
    const response = await decisionOutcomesV2Api.list({
      horizon: '5d',
      finalActionFamily: 'long',
      pageSize: 20,
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/api/v1/decision-signals/outcomes-v2',
      { params: { horizon: '5d', final_action_family: 'long', page_size: 20 } },
    );
    expect(response.contract).toBe('decision-outcome-v2-collection');
    expect(response.pageSize).toBe(20);
    expect(response.items[0].signalId).toBe(42);
    expect(response.items[0].datasetHashes).toEqual(['a'.repeat(64)]);
  });

  it('serializes repeated horizon params and retains null calibration metrics', async () => {
    vi.mocked(apiClient.get).mockResolvedValueOnce({
      data: {
        ...stats,
        buckets: [{
          ...stats.buckets[0],
          accuracy: null,
          ece: null,
          brier_score: null,
        }],
      },
    });
    const response = await decisionOutcomesV2Api.getStats({ horizons: ['5d', '20d'] });
    const options = vi.mocked(apiClient.get).mock.calls[0][1] as {
      paramsSerializer: { serialize: (params: Record<string, unknown>) => string };
    };

    expect(options.paramsSerializer.serialize({ horizons: ['5d', '20d'] }))
      .toBe('horizons=5d&horizons=20d');
    expect(response.buckets[0].accuracy).toBeNull();
    expect(response.buckets[0].brierScore).toBeNull();
  });

  it('rejects v1 or malformed payloads instead of blending contracts', async () => {
    vi.mocked(apiClient.get).mockResolvedValueOnce({
      data: { ...collection, contract: 'decision-signal-outcome-collection' },
    });
    await expect(decisionOutcomesV2Api.list()).rejects.toThrow(
      'Decision Outcome v2 collection is malformed',
    );
  });
});
