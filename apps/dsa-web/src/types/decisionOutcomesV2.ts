export type DecisionOutcomeV2Horizon = '5d' | '10d' | '20d';
export type DecisionOutcomeV2Status =
  | 'pending'
  | 'evaluated'
  | 'observational'
  | 'unexecutable'
  | 'unable';
export type DecisionOutcomeV2ActionFamily = 'long' | 'defensive' | 'observational';

export interface DecisionOutcomeV2BenchmarkItem {
  code: string | null;
  name: string | null;
  status: 'available' | 'unavailable';
  reasonCode: string | null;
  returnPct: number | null;
  stockExcessReturnPct: number | null;
  directionalExcessReturnPct: number | null;
}

export interface DecisionOutcomeV2Item {
  id: number;
  signalId: number;
  outcomeContract: 'decision-outcome-v2';
  horizon: DecisionOutcomeV2Horizon;
  engineVersion: string;
  evalStatus: DecisionOutcomeV2Status;
  finalActionFamily: DecisionOutcomeV2ActionFamily;
  outcome: 'hit' | 'miss' | null;
  directionCorrect: boolean | null;
  reasonCode: string | null;
  executionStatus: 'pending' | 'executable' | 'unexecutable' | 'unavailable';
  signalCreatedAt: string;
  signalSession: string | null;
  stockCode: string;
  market: string;
  sourceType: string;
  signalAction: string;
  signalHorizon: string | null;
  signalStatus: string;
  decisionProfile: string;
  researchSnapshotHash: string;
  policyVersion: string;
  policyHash: string;
  policyEvaluationHash: string;
  portfolioSnapshotRef: string;
  promptVersion: string | null;
  researchStance: string;
  proposedAccountAction: string;
  finalAccountAction: string;
  policyMode: string;
  policyVerdict: string;
  policyAllowed: boolean;
  wouldBlock: boolean;
  confidence: number | null;
  signalScore: number | null;
  valueQualityScore: number;
  trendTimingScore: number;
  catalystScore: number;
  riskScore: number;
  evidenceQualityScore: number;
  entryTradeDate: string | null;
  entryRawOpen: number | null;
  entryAdjFactor: number | null;
  endTradeDate: string | null;
  tradingDayCount: number | null;
  endAdjustedClose: number | null;
  stockReturnPct: number | null;
  directionalReturnPct: number | null;
  mfePct: number | null;
  maePct: number | null;
  csi300: DecisionOutcomeV2BenchmarkItem;
  sw1: DecisionOutcomeV2BenchmarkItem;
  datasetHashes: string[];
  observationHash: string;
  evaluatedAt: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DecisionOutcomeV2ListParams {
  signalId?: number;
  horizon?: DecisionOutcomeV2Horizon;
  engineVersion?: string;
  evalStatus?: DecisionOutcomeV2Status;
  finalActionFamily?: DecisionOutcomeV2ActionFamily;
  decisionProfile?: string;
  stockCode?: string;
  page?: number;
  pageSize?: number;
}

export interface DecisionOutcomeV2ListResponse {
  contract: 'decision-outcome-v2-collection';
  version: 'v1';
  items: DecisionOutcomeV2Item[];
  total: number;
  page: number;
  pageSize: number;
}

export interface DecisionOutcomeV2CalibrationBin {
  index: number;
  lowerBound: number;
  upperBound: number;
  upperInclusive: boolean;
  count: number;
  averageConfidence: number | null;
  empiricalAccuracy: number | null;
  absoluteGap: number | null;
}

export interface DecisionOutcomeV2BenchmarkStats {
  samples: number;
  sampleSufficient: boolean;
  calibrationSamples: number;
  calibrationSampleSufficient: boolean;
  accuracy: number | null;
  ece: number | null;
  brierScore: number | null;
  bins: DecisionOutcomeV2CalibrationBin[];
  meanDirectionalExcessReturnPct: number | null;
  outperformanceRate: number | null;
  nonUnderperformanceRate: number | null;
}

export interface DecisionOutcomeV2StatsBucket {
  dimensions: {
    engine: string;
    horizon: DecisionOutcomeV2Horizon;
    profile: string;
    finalActionFamily: DecisionOutcomeV2ActionFamily;
  };
  total: number;
  evaluatedDirectionalOutcomes: number;
  calibrationSamples: number;
  sampleSufficient: boolean;
  accuracy: number | null;
  ece: number | null;
  brierScore: number | null;
  bins: DecisionOutcomeV2CalibrationBin[];
  benchmarks: {
    csi300: DecisionOutcomeV2BenchmarkStats;
    sw1: DecisionOutcomeV2BenchmarkStats;
  };
}

export interface DecisionOutcomeV2StatsParams {
  horizons?: DecisionOutcomeV2Horizon[];
  engineVersion?: string;
  decisionProfile?: string;
  finalActionFamily?: DecisionOutcomeV2ActionFamily;
}

export interface DecisionOutcomeV2StatsResponse {
  contract: 'decision-outcome-v2-stats';
  version: 'v1';
  engineVersion: string;
  horizons: DecisionOutcomeV2Horizon[];
  bucketDimensions: string[];
  minimumCompletedSampleSize: number;
  calibrationBinCount: number;
  buckets: DecisionOutcomeV2StatsBucket[];
}
