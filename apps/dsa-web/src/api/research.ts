import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  JsonValue,
  PersonalResearchDebateReviewResponse,
  PersonalResearchLatestThesisParams,
  PersonalResearchSkillExecutionListResponse,
  PersonalResearchSkillExecutionResponse,
  PersonalResearchSkillId,
  PersonalResearchThesisResponse,
  ResearchDatasetItem,
  ResearchDatasetListResponse,
  ResearchDatasetParams,
  ResearchDebateDetailResponse,
  ResearchDebateListParams,
  ResearchDebateListResponse,
  ResearchEvidenceDetailResponse,
  ResearchEvidenceListParams,
  ResearchEvidenceListResponse,
  ResearchFactorParams,
  ResearchFactorResponse,
  ResearchSnapshotResponse,
  ResearchUniverseResponse,
  ResearchWatchlistDeleteResponse,
  ResearchWatchlistItem,
  ResearchWatchlistMarket,
  ResearchWatchlistMetadataInput,
  ResearchWatchlistResponse,
  ResearchWatchlistSource,
} from '../types/research';

const RESEARCH_WATCHLIST_SOURCES = new Set<ResearchWatchlistSource>([
  'enhanced',
  'legacy',
  'holding',
]);

const PERSONAL_RESEARCH_SKILL_IDS: PersonalResearchSkillId[] = [
  'personal-value-quality',
  'personal-trend-timing',
  'personal-catalyst',
  'personal-risk',
  'personal-evidence-quality',
];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

function omitUndefined(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(input).filter(([, value]) => value !== undefined),
  );
}

function encodePathSegment(value: string): string {
  return encodeURIComponent(value);
}

function toFactorParams(params: ResearchFactorParams): Record<string, unknown> {
  return omitUndefined({
    as_of: params.asOf,
    horizon_days: params.horizonDays,
  });
}

function toDatasetParams<TDetail extends boolean>(
  params: ResearchDatasetParams<TDetail>,
): Record<string, unknown> {
  return omitUndefined({
    dataset: params.dataset,
    as_of: params.asOf,
    detail: params.detail,
    limit: params.limit,
    row_limit: params.rowLimit,
  });
}

function toResearchFactorResponse(data: Record<string, unknown>): ResearchFactorResponse {
  const response = toCamelCase<ResearchFactorResponse>(data);
  response.factors = data.factors as Record<string, JsonValue>;
  response.requestedTrend = data.requested_trend as Record<string, JsonValue> | null;
  response.unknowns = data.unknowns as JsonValue;
  return response;
}

function toResearchSnapshotResponse(data: Record<string, unknown>): ResearchSnapshotResponse {
  const response = toCamelCase<ResearchSnapshotResponse>(data);
  response.snapshot = data.snapshot as Record<string, JsonValue>;
  return response;
}

function toResearchDatasetItem(
  data: Record<string, unknown>,
): ResearchDatasetItem<JsonValue> {
  const item = toCamelCase<ResearchDatasetItem<JsonValue>>(data);
  item.normalized = data.normalized as JsonValue | null;
  item.rawRef = data.raw_ref as Record<string, JsonValue> | null;
  return item;
}

function toResearchDatasetListResponse<TDetail extends boolean>(
  data: Record<string, unknown>,
): ResearchDatasetListResponse<TDetail> {
  if (!Array.isArray(data.items)) {
    throw new Error('Research dataset list response items must be an array');
  }
  const response = toCamelCase<ResearchDatasetListResponse<TDetail>>(data);
  response.items = data.items.map((item) => toResearchDatasetItem(
    item as Record<string, unknown>,
  )) as ResearchDatasetListResponse<TDetail>['items'];
  return response;
}

function toResearchEvidenceListResponse(
  data: Record<string, unknown>,
): ResearchEvidenceListResponse {
  if (!Array.isArray(data.items)) {
    throw new Error('Research evidence list response items must be an array');
  }
  const response = toCamelCase<ResearchEvidenceListResponse>(data);
  response.items = data.items.map((item) => (
    toCamelCase(item as Record<string, unknown>)
  ));
  return response;
}

function toResearchEvidenceDetailResponse(
  data: Record<string, unknown>,
): ResearchEvidenceDetailResponse {
  const response = toCamelCase<ResearchEvidenceDetailResponse>(data);
  if (!response.evidence || !Array.isArray(response.evidence.claims)
    || !Array.isArray(response.evidence.citations)) {
    throw new Error('Research evidence detail response is malformed');
  }
  return response;
}

function toResearchDebateListResponse(
  data: Record<string, unknown>,
): ResearchDebateListResponse {
  if (!Array.isArray(data.items)) {
    throw new Error('Research debate list response items must be an array');
  }
  const response = toCamelCase<ResearchDebateListResponse>(data);
  response.items = data.items.map((item) => (
    toCamelCase(item as Record<string, unknown>)
  ));
  return response;
}

function toResearchDebateDetailResponse(
  data: Record<string, unknown>,
): ResearchDebateDetailResponse {
  const response = toCamelCase<ResearchDebateDetailResponse>(data);
  if (!response.debate || !Array.isArray(response.debate.failedStances)
    || response.debate.failedStances.some((failure) => (
      !failure
      || typeof failure !== 'object'
      || (failure.stance !== 'bull' && failure.stance !== 'bear')
      || typeof failure.errorCode !== 'string'
    ))
    || !Array.isArray(response.debate.limitations)
    || !Array.isArray(response.debate.turns)
    || response.debate.turns.some((turn) => (
      !Array.isArray(turn.arguments)
      || !Array.isArray(turn.openQuestions)
      || turn.arguments.some((argument) => (
        !Array.isArray(argument.claimIds)
        || !Array.isArray(argument.citationIds)
        || !Array.isArray(argument.limitations)
      ))
    ))) {
    throw new Error('Research debate detail response is malformed');
  }
  return response;
}

function toPersonalResearchSkillExecution(
  data: Record<string, unknown>,
): PersonalResearchSkillExecutionResponse {
  const response = toCamelCase<PersonalResearchSkillExecutionResponse>(data);
  if (response.contract !== 'personal-research-skill-execution'
    || response.version !== 'v1'
    || !PERSONAL_RESEARCH_SKILL_IDS.includes(response.skillContract?.skillId)
    || !isStringArray(response.lineage?.datasetSnapshotHashes)
    || !isRecord(response.input)
    || !isRecord(response.result?.output)
    || (response.result?.status !== 'succeeded' && response.result?.status !== 'failed')) {
    throw new Error('Personal Research Skill execution response is malformed');
  }
  return response;
}

function toPersonalResearchSkillExecutionListResponse(
  data: Record<string, unknown>,
): PersonalResearchSkillExecutionListResponse {
  if (!Array.isArray(data.executions)) {
    throw new Error('Personal Research Skill execution collection is malformed');
  }
  const response = toCamelCase<PersonalResearchSkillExecutionListResponse>(data);
  if (response.contract !== 'personal-research-skill-execution-collection'
    || response.version !== 'v1'
    || !isStringArray(response.expectedSkillIds)
    || !isStringArray(response.missingSkillIds)) {
    throw new Error('Personal Research Skill execution collection is malformed');
  }
  response.executions = data.executions.map((execution) => {
    if (!isRecord(execution)) {
      throw new Error('Personal Research Skill execution collection is malformed');
    }
    return toPersonalResearchSkillExecution(execution);
  });
  return response;
}

function toPersonalResearchDebateReviewResponse(
  data: Record<string, unknown>,
): PersonalResearchDebateReviewResponse {
  const response = toCamelCase<PersonalResearchDebateReviewResponse>(data);
  if (response.contract !== 'personal-research-debate-review'
    || response.version !== 'v1'
    || !isStringArray(response.verifier?.reasonCodes)
    || !isStringArray(response.judge?.reasonCodes)
    || !isRecord(response.verifier?.input)
    || !isRecord(response.verifier?.output)
    || !isRecord(response.judge?.input)
    || !isRecord(response.judge?.output)) {
    throw new Error('Personal Research Debate review response is malformed');
  }
  return response;
}

function toPersonalResearchThesisResponse(
  data: Record<string, unknown>,
): PersonalResearchThesisResponse {
  const rawLineage = data.lineage;
  const rawSkillHashes = isRecord(rawLineage)
    ? rawLineage.skill_execution_hashes
    : null;
  if (!isRecord(rawSkillHashes)) {
    throw new Error('Personal Research Thesis response is malformed');
  }
  const skillExecutionHashes = Object.fromEntries(
    PERSONAL_RESEARCH_SKILL_IDS.map((skillId) => [skillId, rawSkillHashes[skillId]]),
  );
  if (Object.values(skillExecutionHashes).some((hash) => typeof hash !== 'string')) {
    throw new Error('Personal Research Thesis response is malformed');
  }

  const response = toCamelCase<PersonalResearchThesisResponse>(data);
  if (response.contract !== 'personal-research-thesis'
    || !response.version
    || !isStringArray(response.catalysts)
    || !isStringArray(response.invalidators)
    || !isStringArray(response.unknowns)
    || !isStringArray(response.evidenceRefs)
    || !isRecord(response.content)
    || !isRecord(response.scores)) {
    throw new Error('Personal Research Thesis response is malformed');
  }
  response.lineage.skillExecutionHashes = skillExecutionHashes as
    PersonalResearchThesisResponse['lineage']['skillExecutionHashes'];
  return response;
}

function toResearchWatchlistItem(
  data: Record<string, unknown>,
): ResearchWatchlistItem {
  const item = toCamelCase<ResearchWatchlistItem>(data);
  if (!Array.isArray(item.sources)) {
    throw new Error('Research watchlist item sources must be an array');
  }
  if (item.sources.some((source) => !RESEARCH_WATCHLIST_SOURCES.has(source))) {
    throw new Error('Research watchlist item contains an unknown source');
  }
  return item;
}

function toResearchWatchlistResponse<TResponse extends ResearchWatchlistResponse>(
  data: Record<string, unknown>,
): TResponse {
  if (!Array.isArray(data.items)) {
    throw new Error('Research watchlist response items must be an array');
  }
  const response = toCamelCase<TResponse>(data);
  response.items = data.items.map((item) => (
    toResearchWatchlistItem(item as Record<string, unknown>)
  ));
  if (response.holdingsFreshness !== 'ledger') {
    throw new Error('Research watchlist response must confirm ledger freshness');
  }
  return response;
}

function toWatchlistMetadataPayload(
  metadata: ResearchWatchlistMetadataInput,
): Record<string, unknown> {
  return {
    reason: metadata.reason,
    priority: metadata.priority,
    analysis_tier: metadata.analysisTier,
    next_review_at: metadata.nextReviewAt,
  };
}

function toEvidenceParams(params: ResearchEvidenceListParams): Record<string, unknown> {
  return omitUndefined({
    job_id: params.jobId,
    research_snapshot_hash: params.researchSnapshotHash,
    stock_code: params.stockCode,
    as_of: params.asOf,
    cursor: params.cursor,
    limit: params.limit,
  });
}

function toDebateParams(params: ResearchDebateListParams): Record<string, unknown> {
  return omitUndefined({
    job_id: params.jobId,
    research_snapshot_hash: params.researchSnapshotHash,
    stock_code: params.stockCode,
    evidence_snapshot_hash: params.evidenceSnapshotHash,
    as_of: params.asOf,
    cursor: params.cursor,
    limit: params.limit,
  });
}

async function listDatasets(
  stockCode: string,
  params: ResearchDatasetParams<true>,
): Promise<ResearchDatasetListResponse<true>>;
async function listDatasets(
  stockCode: string,
  params?: ResearchDatasetParams<false>,
): Promise<ResearchDatasetListResponse<false>>;
async function listDatasets(
  stockCode: string,
  params: ResearchDatasetParams<boolean>,
): Promise<ResearchDatasetListResponse<boolean>>;
async function listDatasets(
  stockCode: string,
  params: ResearchDatasetParams<boolean> = {},
): Promise<ResearchDatasetListResponse<boolean>> {
  const response = await apiClient.get<Record<string, unknown>>(
    `/api/v1/research/datasets/${encodePathSegment(stockCode)}`,
    { params: toDatasetParams(params) },
  );
  return toResearchDatasetListResponse<boolean>(response.data);
}

export const researchApi = {
  async getFactors(
    stockCode: string,
    params: ResearchFactorParams = {},
  ): Promise<ResearchFactorResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/factors/${encodePathSegment(stockCode)}`,
      { params: toFactorParams(params) },
    );
    return toResearchFactorResponse(response.data);
  },

  async getSnapshot(snapshotHash: string): Promise<ResearchSnapshotResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/snapshots/${encodePathSegment(snapshotHash)}`,
    );
    return toResearchSnapshotResponse(response.data);
  },

  async listEvidence(
    params: ResearchEvidenceListParams,
  ): Promise<ResearchEvidenceListResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/research/evidence',
      { params: toEvidenceParams(params) },
    );
    return toResearchEvidenceListResponse(response.data);
  },

  async getEvidence(
    evidenceHash: string,
  ): Promise<ResearchEvidenceDetailResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/evidence/${encodePathSegment(evidenceHash)}`,
    );
    return toResearchEvidenceDetailResponse(response.data);
  },

  async listDebates(
    params: ResearchDebateListParams,
  ): Promise<ResearchDebateListResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/research/debates',
      { params: toDebateParams(params) },
    );
    return toResearchDebateListResponse(response.data);
  },

  async getDebate(
    debateHash: string,
  ): Promise<ResearchDebateDetailResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/debates/${encodePathSegment(debateHash)}`,
    );
    return toResearchDebateDetailResponse(response.data);
  },

  async getPersonalResearchSkillExecution(
    executionHash: string,
  ): Promise<PersonalResearchSkillExecutionResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/personal/artifacts/skills/${encodePathSegment(executionHash)}`,
    );
    return toPersonalResearchSkillExecution(response.data);
  },

  async listPersonalResearchSkillExecutions(
    taskId: string,
    market: string,
    stockCode: string,
  ): Promise<PersonalResearchSkillExecutionListResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      [
        '/api/v1/research/personal/artifacts/skills/tasks',
        encodePathSegment(taskId),
        'stocks',
        encodePathSegment(market),
        encodePathSegment(stockCode),
      ].join('/'),
    );
    return toPersonalResearchSkillExecutionListResponse(response.data);
  },

  async getPersonalResearchDebateReview(
    reviewHash: string,
  ): Promise<PersonalResearchDebateReviewResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/personal/artifacts/debate-reviews/${encodePathSegment(reviewHash)}`,
    );
    return toPersonalResearchDebateReviewResponse(response.data);
  },

  async getLatestPersonalResearchThesisBySignal(
    decisionSignalId: number,
  ): Promise<PersonalResearchThesisResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/personal/artifacts/theses/by-signal/${decisionSignalId}`,
    );
    return toPersonalResearchThesisResponse(response.data);
  },

  async getLatestPersonalResearchThesis(
    params: PersonalResearchLatestThesisParams,
  ): Promise<PersonalResearchThesisResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/research/personal/artifacts/theses/latest',
      {
        params: {
          task_id: params.taskId,
          market: params.market,
          stock_code: params.stockCode,
        },
      },
    );
    return toPersonalResearchThesisResponse(response.data);
  },

  async getPersonalResearchThesis(
    thesisHash: string,
  ): Promise<PersonalResearchThesisResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      `/api/v1/research/personal/artifacts/theses/${encodePathSegment(thesisHash)}`,
    );
    return toPersonalResearchThesisResponse(response.data);
  },

  listDatasets,
};

export const researchWatchlistApi = {
  async getWatchlist(): Promise<ResearchWatchlistResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/research/watchlist',
    );
    return toResearchWatchlistResponse<ResearchWatchlistResponse>(response.data);
  },

  async getUniverse(): Promise<ResearchUniverseResponse> {
    const response = await apiClient.get<Record<string, unknown>>(
      '/api/v1/research/universe',
    );
    return toResearchWatchlistResponse<ResearchUniverseResponse>(response.data);
  },

  async upsertItem(
    market: ResearchWatchlistMarket,
    stockCode: string,
    metadata: ResearchWatchlistMetadataInput,
  ): Promise<ResearchWatchlistItem> {
    const response = await apiClient.put<Record<string, unknown>>(
      `/api/v1/research/watchlist/${encodePathSegment(market)}/${encodePathSegment(stockCode)}`,
      toWatchlistMetadataPayload(metadata),
    );
    return toResearchWatchlistItem(response.data);
  },

  async removeItem(
    market: ResearchWatchlistMarket,
    stockCode: string,
  ): Promise<ResearchWatchlistDeleteResponse> {
    const response = await apiClient.delete<Record<string, unknown>>(
      `/api/v1/research/watchlist/${encodePathSegment(market)}/${encodePathSegment(stockCode)}`,
    );
    const result = toCamelCase<ResearchWatchlistDeleteResponse>(response.data);
    if (result.deleted !== 1) {
      throw new Error('Research watchlist delete response did not confirm deletion');
    }
    return result;
  },
};
