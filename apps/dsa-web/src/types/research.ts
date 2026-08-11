export type JsonPrimitive = string | number | boolean | null;

export type JsonValue =
  | JsonPrimitive
  | JsonValue[]
  | { [key: string]: JsonValue };

export type ResearchDataStatus =
  | 'available'
  | 'empty'
  | 'partial'
  | 'stale'
  | 'permission_denied'
  | 'not_supported'
  | 'fetch_failed';

export type ResearchHorizonDays = 5 | 10 | 20;

export interface ResearchFactorParams {
  asOf?: string;
  horizonDays?: ResearchHorizonDays;
}

export interface ResearchFactorResponse {
  id: number;
  stockCode: string;
  market: string;
  companyProfile: string;
  primaryHorizon: number;
  requestedHorizon: ResearchHorizonDays;
  requestedTrend: Record<string, JsonValue> | null;
  engineBundleVersion: string;
  valueScore: number | null;
  qualityScore: number | null;
  trendScore: number | null;
  catalystScore: number | null;
  riskPenalty: number | null;
  factors: Record<string, JsonValue>;
  inputDatasetHashes: string[];
  status: ResearchDataStatus;
  coverage: number;
  unknowns: JsonValue;
  asOf: string;
  availableAt: string;
  contentHash: string;
  originJobId: string | null;
  createdAt: string;
}

export interface ResearchSnapshotResponse {
  id: number;
  stockCode: string;
  market: string;
  snapshotVersion: string;
  fieldDictionaryVersion: string;
  factorEngineVersion: string;
  packVersion: string;
  promptVersion: string;
  policyVersion: string;
  modelRouteFingerprint: string;
  asOf: string;
  availableAt: string;
  status: ResearchDataStatus;
  snapshot: Record<string, JsonValue>;
  snapshotHash: string;
  factorSnapshotHash: string | null;
  evidenceSnapshotHash: string | null;
  debateSnapshotHash: string | null;
  originJobId: string | null;
  createdAt: string;
}

interface ResearchDatasetBaseParams {
  dataset?: string;
  asOf?: string;
  limit?: number;
  rowLimit?: number;
}

export type ResearchDatasetParams<TDetail extends boolean = false> =
  ResearchDatasetBaseParams & (
    boolean extends TDetail
      ? { detail?: boolean }
      : TDetail extends true
        ? { detail: true }
        : { detail?: false }
  );

export type ResearchDatasetListSummary = {
  row_count: number;
  fields: string[];
};

export type ResearchDatasetObjectSummary = {
  fields: string[];
};

export type ResearchDatasetSummaryValue =
  | JsonPrimitive
  | ResearchDatasetListSummary
  | ResearchDatasetObjectSummary;

export interface ResearchDatasetItem<TNormalized extends JsonValue = JsonValue> {
  id: number;
  dataset: string;
  scopeType: string;
  scopeValue: string;
  market: string;
  provider: string;
  schemaVersion: string;
  tradeDate: string | null;
  reportDate: string | null;
  announcementDate: string | null;
  dataAsOf: string;
  availableAt: string;
  observedAt: string;
  status: ResearchDataStatus;
  normalized: TNormalized | null;
  normalizedRowCount: number | null;
  normalizedTruncated: boolean;
  contentHash: string;
  rawRef: Record<string, JsonValue> | null;
  errorCode: string | null;
  errorMessage: string | null;
  supersedesHash: string | null;
  originJobId: string | null;
  createdAt: string;
}

export interface ResearchDatasetListResponse<
  TDetail extends boolean = false,
> {
  items: Array<ResearchDatasetItem<
    TDetail extends true ? JsonValue : ResearchDatasetSummaryValue
  >>;
  count: number;
  detail: TDetail;
  rowLimit: number;
}

export type ResearchDatasetSummaryResponse = ResearchDatasetListResponse<false>;
export type ResearchDatasetDetailResponse = ResearchDatasetListResponse<true>;
export type ResearchDatasetSummaryItem = ResearchDatasetItem<ResearchDatasetSummaryValue>;
export type ResearchDatasetDetailItem = ResearchDatasetItem<JsonValue>;

export type ResearchEvidenceStatus = 'available' | 'partial' | 'empty' | 'fetch_failed';
export type ResearchEvidenceClaimStatus =
  | 'supported'
  | 'partial'
  | 'contradicted'
  | 'insufficient';
export type ResearchEvidenceClaimKind = 'factor_metric' | 'reported_event';
export type ResearchEvidenceRelation = 'supports' | 'contradicts' | 'context';
export type ResearchEvidenceArtifactType = 'dataset' | 'factor';

export interface ResearchEvidenceCitation {
  id: string;
  relation: ResearchEvidenceRelation;
  artifactType: ResearchEvidenceArtifactType;
  artifactHash: string;
  jsonPointer: string;
  valueHash: string;
  availableAt: string;
  sourceName: string;
  title: string;
  excerpt: string;
  canonicalUrl: string | null;
}

export interface ResearchEvidenceClaim {
  id: string;
  kind: ResearchEvidenceClaimKind;
  statement: string;
  status: ResearchEvidenceClaimStatus;
  citationIds: string[];
  limitations: string[];
  availableAt: string;
}

export interface ResearchEvidencePayload {
  evidenceEngineVersion: string;
  claimPolicyVersion: string;
  stockCode: string;
  market: string;
  asOf: string;
  availableAt: string;
  status: ResearchEvidenceStatus;
  coverage: number;
  inputDatasetHashes: string[];
  factorSnapshotHash: string;
  limitations: string[];
  claims: ResearchEvidenceClaim[];
  citations: ResearchEvidenceCitation[];
}

export interface ResearchEvidenceSummary {
  id: number;
  stockCode: string;
  market: string;
  evidenceEngineVersion: string;
  claimPolicyVersion: string;
  asOf: string;
  availableAt: string;
  status: ResearchEvidenceStatus;
  coverage: number;
  claimCount: number;
  citationCount: number;
  inputDatasetHashes: string[];
  factorSnapshotHash: string;
  evidenceHash: string;
  originJobId: string | null;
  createdAt: string;
}

export interface ResearchEvidenceDetailResponse extends ResearchEvidenceSummary {
  evidence: ResearchEvidencePayload;
}

export interface ResearchEvidenceListResponse {
  items: ResearchEvidenceSummary[];
  count: number;
  nextCursor: string | null;
}

type ResearchEvidenceSelector =
  | {
      jobId: string;
      researchSnapshotHash?: string;
      stockCode?: string;
    }
  | {
      jobId?: string;
      researchSnapshotHash: string;
      stockCode?: string;
    }
  | {
      jobId?: string;
      researchSnapshotHash?: string;
      stockCode: string;
    };

export type ResearchEvidenceListParams = ResearchEvidenceSelector & {
  asOf?: string;
  cursor?: string;
  limit?: number;
};

export type ResearchDebateStatus =
  | 'available'
  | 'partial'
  | 'empty'
  | 'generation_failed';
export type ResearchDebateStance = 'bull' | 'bear';

export interface ResearchDebateFailure {
  stance: ResearchDebateStance;
  errorCode: string;
}

export interface ResearchDebateArgument {
  id: string;
  statement: string;
  claimIds: string[];
  citationIds: string[];
  confidence: number;
  limitations: string[];
}

export interface ResearchDebateTurn {
  turnHash: string;
  promptFingerprint: string;
  modelUsed: string;
  stance: ResearchDebateStance;
  summary: string;
  arguments: ResearchDebateArgument[];
  openQuestions: string[];
}

export interface ResearchDebatePayload {
  debateEngineVersion: string;
  outputSchemaVersion: string;
  promptVersion: string;
  stockCode: string;
  market: string;
  asOf: string;
  availableAt: string;
  status: ResearchDebateStatus;
  evidenceSnapshotHash: string;
  requestHash: string;
  modelRouteFingerprint: string;
  bullTurnHash: string | null;
  bearTurnHash: string | null;
  failedStances: ResearchDebateFailure[];
  limitations: string[];
  turns: ResearchDebateTurn[];
}

export interface ResearchDebateSummary {
  id: number;
  stockCode: string;
  market: string;
  debateEngineVersion: string;
  outputSchemaVersion: string;
  promptVersion: string;
  evidenceSnapshotHash: string;
  requestHash: string;
  modelRouteFingerprint: string;
  asOf: string;
  availableAt: string;
  status: ResearchDebateStatus;
  bullTurnHash: string | null;
  bearTurnHash: string | null;
  bullArgumentCount: number;
  bearArgumentCount: number;
  openQuestionCount: number;
  debateHash: string;
  originJobId: string | null;
  createdAt: string;
}

export interface ResearchDebateDetailResponse extends ResearchDebateSummary {
  debate: ResearchDebatePayload;
}

export interface ResearchDebateListResponse {
  items: ResearchDebateSummary[];
  count: number;
  nextCursor: string | null;
}

export type ResearchWatchlistAnalysisTier = 'quick' | 'standard' | 'deep';
export type ResearchWatchlistMarket = 'cn' | 'hk' | 'us' | 'jp' | 'kr' | 'tw';
export type ResearchWatchlistSource = 'enhanced' | 'legacy' | 'holding';

export type ResearchHoldingsFreshness = 'ledger' | null;

export interface ResearchWatchlistItem {
  stockCode: string;
  market: ResearchWatchlistMarket;
  sources: ResearchWatchlistSource[];
  reason: string | null;
  priority: number;
  analysisTier: ResearchWatchlistAnalysisTier;
  nextReviewAt: string | null;
  isActive: boolean;
  isHolding: boolean;
}

export interface ResearchWatchlistResponse {
  items: ResearchWatchlistItem[];
  holdingsFreshness: 'ledger';
}

export type ResearchUniverseResponse = ResearchWatchlistResponse;

export interface ResearchWatchlistMetadataInput {
  reason: string | null;
  priority: number;
  analysisTier: ResearchWatchlistAnalysisTier;
  nextReviewAt: string | null;
}

export interface ResearchWatchlistDeleteResponse {
  deleted: 1;
}

type ResearchDebateSelector =
  | {
      jobId: string;
      researchSnapshotHash?: string;
      stockCode?: string;
    }
  | {
      jobId?: string;
      researchSnapshotHash: string;
      stockCode?: string;
    }
  | {
      jobId?: string;
      researchSnapshotHash?: string;
      stockCode: string;
    };

export type ResearchDebateListParams = ResearchDebateSelector & {
  evidenceSnapshotHash?: string;
  asOf?: string;
  cursor?: string;
  limit?: number;
};

export type PersonalResearchSkillId =
  | 'personal-value-quality'
  | 'personal-trend-timing'
  | 'personal-catalyst'
  | 'personal-risk'
  | 'personal-evidence-quality';

export interface PersonalResearchStockLineage {
  taskId: string;
  market: string;
  stockCode: string;
}

export interface PersonalResearchSkillContract {
  skillId: PersonalResearchSkillId;
  version: string;
  contractHash: string;
  scoreField: string;
}

export interface PersonalResearchSkillLineage extends PersonalResearchStockLineage {
  researchSnapshotHash: string;
  factorSnapshotHash: string;
  evidenceSnapshotHash: string;
  datasetSnapshotHashes: string[];
  datasetLineageHash: string;
  inputHash: string;
  outputHash: string;
}

export interface PersonalResearchSkillResult {
  status: 'succeeded' | 'failed';
  score: number | null;
  output: Record<string, JsonValue>;
}

export interface PersonalResearchSkillExecutionResponse {
  contract: 'personal-research-skill-execution';
  version: 'v1';
  executionHash: string;
  skillContract: PersonalResearchSkillContract;
  lineage: PersonalResearchSkillLineage;
  input: Record<string, JsonValue>;
  result: PersonalResearchSkillResult;
  createdAt: string;
}

export interface PersonalResearchSkillExecutionListResponse {
  contract: 'personal-research-skill-execution-collection';
  version: 'v1';
  lineage: PersonalResearchStockLineage;
  expectedSkillIds: PersonalResearchSkillId[];
  missingSkillIds: PersonalResearchSkillId[];
  complete: boolean;
  executions: PersonalResearchSkillExecutionResponse[];
}

export interface PersonalResearchDebateVerifier {
  version: string;
  inputHash: string;
  outputHash: string;
  valid: boolean;
  failClosed: boolean;
  reasonCodes: string[];
  input: Record<string, JsonValue>;
  output: Record<string, JsonValue>;
}

export type PersonalResearchDebateVerdict = 'bull' | 'bear' | 'balanced' | 'fail_closed';

export interface PersonalResearchDebateJudge {
  version: string;
  policyHash: string;
  inputHash: string;
  outputHash: string;
  failClosed: boolean;
  reasonCodes: string[];
  verdict: PersonalResearchDebateVerdict;
  winner: 'bull' | 'bear' | null;
  input: Record<string, JsonValue>;
  output: Record<string, JsonValue>;
}

export interface PersonalResearchDebateReviewResponse {
  contract: 'personal-research-debate-review';
  version: 'v1';
  reviewHash: string;
  lineage: PersonalResearchStockLineage & {
    debateSnapshotHash: string;
    evidenceSnapshotHash: string;
  };
  verifier: PersonalResearchDebateVerifier;
  judge: PersonalResearchDebateJudge;
  createdAt: string;
}

export type PersonalResearchThesisStance =
  | 'strong_bullish'
  | 'bullish'
  | 'watch'
  | 'neutral'
  | 'bearish'
  | 'avoid';

export type PersonalResearchAccountAction =
  | 'observe'
  | 'open_candidate'
  | 'add_candidate'
  | 'hold'
  | 'reduce_candidate'
  | 'exit_candidate';

export type PersonalResearchSkillExecutionHashes = Record<PersonalResearchSkillId, string>;

export interface PersonalResearchThesisLineage extends PersonalResearchStockLineage {
  researchSnapshotHash: string;
  skillExecutionHashes: PersonalResearchSkillExecutionHashes;
  debateSnapshotHash: string | null;
  debateReviewHash: string | null;
  decisionSignalId: number | null;
  policyEvaluationHash: string | null;
  policyVersion: string | null;
  policyHash: string | null;
  portfolioSnapshotRef: string | null;
  supersedesThesisHash: string | null;
}

export interface PersonalResearchThesisScores {
  valueQualityScore: number;
  trendTimingScore: number;
  catalystScore: number;
  riskScore: number;
  evidenceQualityScore: number;
}

export interface PersonalResearchThesisResponse {
  contract: 'personal-research-thesis';
  version: string;
  thesisHash: string;
  contentHash: string;
  lineage: PersonalResearchThesisLineage;
  stance: PersonalResearchThesisStance;
  accountAction: PersonalResearchAccountAction;
  scores: PersonalResearchThesisScores;
  catalysts: string[];
  invalidators: string[];
  unknowns: string[];
  evidenceRefs: string[];
  content: Record<string, JsonValue>;
  createdAt: string;
}

export interface PersonalResearchLatestThesisParams {
  taskId: string;
  market: string;
  stockCode: string;
}
