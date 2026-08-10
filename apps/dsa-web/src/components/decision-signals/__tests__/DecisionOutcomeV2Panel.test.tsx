import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { UiLanguageProvider } from '../../../contexts/UiLanguageContext';
import { useDecisionOutcomesV2 } from '../../../hooks/useDecisionOutcomesV2';
import type {
  DecisionOutcomeV2Item,
  DecisionOutcomeV2StatsBucket,
} from '../../../types/decisionOutcomesV2';
import { DecisionOutcomeV2Panel } from '../DecisionOutcomeV2Panel';

vi.mock('../../../hooks/useDecisionOutcomesV2', () => ({
  useDecisionOutcomesV2: vi.fn(),
}));

const item = {
  id: 1,
  signalId: 42,
  outcomeContract: 'decision-outcome-v2',
  horizon: '5d',
  evalStatus: 'unable',
  finalActionFamily: 'long',
  decisionProfile: 'balanced',
  stockCode: '600519',
  executionStatus: 'unavailable',
  entryTradeDate: null,
  directionalReturnPct: null,
  mfePct: null,
  maePct: null,
  reasonCode: 'insufficient_xshg_sessions',
  csi300: {
    code: '000300.SH',
    name: 'CSI 300',
    status: 'unavailable',
    reasonCode: 'not_evaluated',
    returnPct: null,
    stockExcessReturnPct: null,
    directionalExcessReturnPct: null,
  },
  sw1: {
    code: null,
    name: null,
    status: 'unavailable',
    reasonCode: 'membership_unavailable',
    returnPct: null,
    stockExcessReturnPct: null,
    directionalExcessReturnPct: null,
  },
} as unknown as DecisionOutcomeV2Item;

const bucket = {
  dimensions: {
    engine: 'personal-research-outcome-v2',
    horizon: '5d',
    profile: 'balanced',
    finalActionFamily: 'long',
  },
  total: 12,
  evaluatedDirectionalOutcomes: 9,
  calibrationSamples: 9,
  sampleSufficient: false,
  accuracy: null,
  ece: null,
  brierScore: null,
  bins: [],
  benchmarks: {
    csi300: { samples: 8, meanDirectionalExcessReturnPct: null },
    sw1: { samples: 7, meanDirectionalExcessReturnPct: null },
  },
} as unknown as DecisionOutcomeV2StatsBucket;

describe('DecisionOutcomeV2Panel', () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem('dsa.uiLanguage', 'en');
    vi.mocked(useDecisionOutcomesV2).mockReturnValue({
      items: [item],
      total: 1,
      stats: {
        contract: 'decision-outcome-v2-stats',
        version: 'v1',
        engineVersion: 'personal-research-outcome-v2',
        horizons: ['5d', '10d', '20d'],
        bucketDimensions: ['engine', 'horizon', 'profile', 'final_action_family'],
        minimumCompletedSampleSize: 30,
        calibrationBinCount: 5,
        buckets: [bucket],
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    });
  });

  it('shows T+1, 5d, benchmark reasons, MFE/MAE, and n/30 without null-to-zero coercion', () => {
    render(<UiLanguageProvider><DecisionOutcomeV2Panel /></UiLanguageProvider>);

    expect(screen.getByText('600519 · 5d')).toBeInTheDocument();
    expect(screen.getByText('n=9/30')).toBeInTheDocument();
    expect(screen.getByText('T+1 executability')).toBeInTheDocument();
    expect(screen.getByText('MFE')).toBeInTheDocument();
    expect(screen.getByText('MAE')).toBeInTheDocument();
    expect(screen.getByText('CSI 300: not_evaluated')).toBeInTheDocument();
    expect(screen.getByText('SW1: membership_unavailable')).toBeInTheDocument();
    expect(screen.getByText(/insufficient_xshg_sessions/)).toBeInTheDocument();
    expect(screen.queryByText('0%')).not.toBeInTheDocument();
  });
});
