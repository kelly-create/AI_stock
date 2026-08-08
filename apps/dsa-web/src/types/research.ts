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
