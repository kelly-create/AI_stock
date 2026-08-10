import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  DecisionOutcomeV2Item,
  DecisionOutcomeV2ListParams,
  DecisionOutcomeV2ListResponse,
  DecisionOutcomeV2StatsParams,
  DecisionOutcomeV2StatsResponse,
} from '../types/decisionOutcomesV2';

function omitUndefined(input: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(input).filter(([, value]) => value !== undefined),
  );
}

function serializeRepeatedQueryParams(params: Record<string, unknown>): string {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    const values = Array.isArray(value) ? value : [value];
    for (const item of values) {
      if (item === undefined || item === null || item === '') continue;
      searchParams.append(key, String(item));
    }
  }
  return searchParams.toString();
}

function toListParams(params: DecisionOutcomeV2ListParams): Record<string, unknown> {
  return omitUndefined({
    signal_id: params.signalId,
    horizon: params.horizon,
    engine_version: params.engineVersion,
    eval_status: params.evalStatus,
    final_action_family: params.finalActionFamily,
    decision_profile: params.decisionProfile,
    stock_code: params.stockCode,
    page: params.page,
    page_size: params.pageSize,
  });
}

function toStatsParams(params: DecisionOutcomeV2StatsParams): Record<string, unknown> {
  return omitUndefined({
    horizons: params.horizons,
    engine_version: params.engineVersion,
    decision_profile: params.decisionProfile,
    final_action_family: params.finalActionFamily,
  });
}

function assertRecord(value: unknown, message: string): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(message);
  }
}

function toOutcomeItem(value: unknown): DecisionOutcomeV2Item {
  assertRecord(value, 'Decision Outcome v2 item must be an object');
  const item = toCamelCase<DecisionOutcomeV2Item>(value);
  if (item.outcomeContract !== 'decision-outcome-v2'
    || !Array.isArray(item.datasetHashes)
    || item.datasetHashes.some((hash) => typeof hash !== 'string')
    || !item.csi300 || !item.sw1) {
    throw new Error('Decision Outcome v2 item is malformed');
  }
  return item;
}

function toListResponse(value: unknown): DecisionOutcomeV2ListResponse {
  assertRecord(value, 'Decision Outcome v2 collection must be an object');
  const response = toCamelCase<DecisionOutcomeV2ListResponse>(value);
  if (response.contract !== 'decision-outcome-v2-collection'
    || response.version !== 'v1'
    || !Array.isArray(value.items)) {
    throw new Error('Decision Outcome v2 collection is malformed');
  }
  response.items = value.items.map(toOutcomeItem);
  return response;
}

function toStatsResponse(value: unknown): DecisionOutcomeV2StatsResponse {
  assertRecord(value, 'Decision Outcome v2 stats must be an object');
  const response = toCamelCase<DecisionOutcomeV2StatsResponse>(value);
  if (response.contract !== 'decision-outcome-v2-stats'
    || response.version !== 'v1'
    || !Array.isArray(response.horizons)
    || !Array.isArray(response.bucketDimensions)
    || !Array.isArray(response.buckets)
    || response.buckets.some((bucket) => (
      !bucket.dimensions
      || !Array.isArray(bucket.bins)
      || !bucket.benchmarks?.csi300
      || !bucket.benchmarks?.sw1
    ))) {
    throw new Error('Decision Outcome v2 stats are malformed');
  }
  return response;
}

export const decisionOutcomesV2Api = {
  async list(
    params: DecisionOutcomeV2ListParams = {},
  ): Promise<DecisionOutcomeV2ListResponse> {
    const response = await apiClient.get<unknown>('/api/v1/decision-signals/outcomes-v2', {
      params: toListParams(params),
    });
    return toListResponse(response.data);
  },

  async listForSignal(signalId: number): Promise<DecisionOutcomeV2ListResponse> {
    const response = await apiClient.get<unknown>(
      `/api/v1/decision-signals/${signalId}/outcomes-v2`,
    );
    return toListResponse(response.data);
  },

  async getStats(
    params: DecisionOutcomeV2StatsParams = {},
  ): Promise<DecisionOutcomeV2StatsResponse> {
    const response = await apiClient.get<unknown>(
      '/api/v1/decision-signals/outcomes-v2/stats',
      {
        params: toStatsParams(params),
        paramsSerializer: { serialize: serializeRepeatedQueryParams },
      },
    );
    return toStatsResponse(response.data);
  },
};
