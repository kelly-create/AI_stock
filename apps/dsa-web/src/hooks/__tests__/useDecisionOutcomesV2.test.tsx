import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { decisionOutcomesV2Api } from '../../api/decisionOutcomesV2';
import { useDecisionOutcomesV2 } from '../useDecisionOutcomesV2';

vi.mock('../../api/decisionOutcomesV2', () => ({
  decisionOutcomesV2Api: { list: vi.fn(), getStats: vi.fn() },
}));

describe('useDecisionOutcomesV2', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(decisionOutcomesV2Api.list).mockResolvedValue({
      contract: 'decision-outcome-v2-collection',
      version: 'v1',
      items: [],
      total: 0,
      page: 1,
      pageSize: 20,
    });
    vi.mocked(decisionOutcomesV2Api.getStats).mockResolvedValue({
      contract: 'decision-outcome-v2-stats',
      version: 'v1',
      engineVersion: 'personal-research-outcome-v2',
      horizons: ['5d', '10d', '20d'],
      bucketDimensions: ['engine', 'horizon', 'profile', 'final_action_family'],
      minimumCompletedSampleSize: 30,
      calibrationBinCount: 5,
      buckets: [],
    });
  });

  it('loads list and stats together and refetches explicitly', async () => {
    const { result } = renderHook(() => useDecisionOutcomesV2());
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(decisionOutcomesV2Api.list).toHaveBeenCalledWith({ page: 1, pageSize: 20 });
    expect(decisionOutcomesV2Api.getStats).toHaveBeenCalledTimes(1);

    act(() => result.current.refetch());
    await waitFor(() => expect(decisionOutcomesV2Api.getStats).toHaveBeenCalledTimes(2));
  });

  it('does not call either API when disabled', () => {
    const { result } = renderHook(() => useDecisionOutcomesV2({ enabled: false }));
    expect(result.current.isLoading).toBe(false);
    expect(decisionOutcomesV2Api.list).not.toHaveBeenCalled();
    expect(decisionOutcomesV2Api.getStats).not.toHaveBeenCalled();
  });
});
