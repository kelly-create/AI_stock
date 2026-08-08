import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  JsonValue,
  ResearchDatasetItem,
  ResearchDatasetListResponse,
  ResearchDatasetParams,
  ResearchFactorParams,
  ResearchFactorResponse,
  ResearchSnapshotResponse,
} from '../types/research';

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

  listDatasets,
};
