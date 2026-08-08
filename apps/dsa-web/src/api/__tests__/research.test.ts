import { beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import { researchApi } from '../research';
import type {
  ResearchDatasetListResponse,
  ResearchDatasetParams,
} from '../../types/research';

const { get } = vi.hoisted(() => ({
  get: vi.fn(),
}));

vi.mock('../index', () => ({
  default: { get },
}));

describe('researchApi', () => {
  beforeEach(() => {
    get.mockReset();
  });

  it('gets factors with encoded stock code and snake-case query parameters', async () => {
    get.mockResolvedValueOnce({
      data: {
        id: 7,
        stock_code: '600519.SZ / test',
        market: 'cn',
        company_profile: 'industrial',
        primary_horizon: 10,
        requested_horizon: 10,
        requested_trend: { name: 'return_10d', score: 65 },
        engine_bundle_version: 'factor-v1',
        value_score: 81,
        quality_score: 75,
        trend_score: null,
        catalyst_score: 60,
        risk_penalty: 20,
        factors: { raw_metric_name: { nested_key: 1 } },
        input_dataset_hashes: ['a'.repeat(64)],
        status: 'partial',
        coverage: 0.8,
        unknowns: [{ field_name: 'trend_score' }],
        as_of: '2026-08-08T15:00:00+08:00',
        available_at: '2026-08-08T15:05:00+08:00',
        content_hash: 'b'.repeat(64),
        origin_job_id: null,
        created_at: '2026-08-08T15:05:00+08:00',
      },
    });

    const result = await researchApi.getFactors('600519.SZ / test', {
      asOf: '2026-08-08T15:00:00+08:00',
      horizonDays: 10,
    });

    expect(get).toHaveBeenCalledWith(
      '/api/v1/research/factors/600519.SZ%20%2F%20test',
      {
        params: {
          as_of: '2026-08-08T15:00:00+08:00',
          horizon_days: 10,
        },
      },
    );
    expect(result.primaryHorizon).toBe(10);
    expect(result.requestedHorizon).toBe(10);
    expect(result.requestedTrend).toEqual({ name: 'return_10d', score: 65 });
    expect(result.inputDatasetHashes).toEqual(['a'.repeat(64)]);
    expect(result.factors).toEqual({ raw_metric_name: { nested_key: 1 } });
    expect(result.unknowns).toEqual([{ field_name: 'trend_score' }]);
  });

  it('gets a snapshot with an encoded hash and preserves its opaque payload', async () => {
    get.mockResolvedValueOnce({
      data: {
        id: 11,
        stock_code: '600519',
        market: 'cn',
        snapshot_version: 'research-snapshot-v1',
        field_dictionary_version: 'field-v1',
        factor_engine_version: 'factor-v1',
        pack_version: 'pack-v1',
        prompt_version: 'prompt-v1',
        policy_version: 'policy-v1',
        model_route_fingerprint: 'route-v1',
        as_of: '2026-08-08T15:00:00+08:00',
        available_at: '2026-08-08T15:05:00+08:00',
        status: 'available',
        snapshot: { source_payload: { original_key: true } },
        snapshot_hash: 'c'.repeat(64),
        factor_snapshot_hash: 'd'.repeat(64),
        evidence_snapshot_hash: 'e'.repeat(64),
        origin_job_id: 'job-1',
        created_at: '2026-08-08T15:05:00+08:00',
      },
    });

    const result = await researchApi.getSnapshot('hash / version');

    expect(get).toHaveBeenCalledWith(
      '/api/v1/research/snapshots/hash%20%2F%20version',
    );
    expect(result.fieldDictionaryVersion).toBe('field-v1');
    expect(result.factorSnapshotHash).toBe('d'.repeat(64));
    expect(result.evidenceSnapshotHash).toBe('e'.repeat(64));
    expect(result.snapshot).toEqual({
      source_payload: { original_key: true },
    });
  });

  it('gets dataset summaries with as-of and paging limits', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [
          {
            id: 21,
            dataset: 'daily',
            scope_type: 'stock',
            scope_value: '600519',
            market: 'cn',
            provider: 'tushare',
            schema_version: 'v1',
            trade_date: '20260808',
            report_date: null,
            announcement_date: null,
            data_as_of: '2026-08-08T15:00:00+08:00',
            available_at: '2026-08-08T15:05:00+08:00',
            observed_at: '2026-08-08T15:05:00+08:00',
            status: 'available',
            normalized: { row_count: 300, fields: ['trade_date', 'close'] },
            normalized_row_count: 300,
            normalized_truncated: false,
            content_hash: 'e'.repeat(64),
            raw_ref: { object_key: 'daily/raw.json' },
            error_code: null,
            error_message: null,
            supersedes_hash: null,
            origin_job_id: 'job-2',
            created_at: '2026-08-08T15:05:00+08:00',
          },
        ],
        count: 1,
        detail: false,
        row_limit: 250,
      },
    });

    const result = await researchApi.listDatasets('600519', {
      dataset: 'daily',
      asOf: '2026-08-08T15:00:00+08:00',
      limit: 20,
      rowLimit: 250,
    });

    expectTypeOf(result).toEqualTypeOf<ResearchDatasetListResponse<false>>();
    expect(get).toHaveBeenCalledWith('/api/v1/research/datasets/600519', {
      params: {
        dataset: 'daily',
        as_of: '2026-08-08T15:00:00+08:00',
        limit: 20,
        row_limit: 250,
      },
    });
    expect(result.detail).toBe(false);
    expect(result.rowLimit).toBe(250);
    expect(result.items[0].normalized).toEqual({
      row_count: 300,
      fields: ['trade_date', 'close'],
    });
    expect(result.items[0].rawRef).toEqual({ object_key: 'daily/raw.json' });
  });

  it('gets dataset details with row limiting and preserves normalized rows', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [
          {
            id: 22,
            dataset: 'daily',
            scope_type: 'stock',
            scope_value: '600519 / SH',
            market: 'cn',
            provider: 'tushare',
            schema_version: 'v1',
            trade_date: '20260808',
            report_date: null,
            announcement_date: null,
            data_as_of: '2026-08-08T15:00:00+08:00',
            available_at: '2026-08-08T15:05:00+08:00',
            observed_at: '2026-08-08T15:05:00+08:00',
            status: 'available',
            normalized: [{ trade_date: '20260808', adj_factor: 1.2 }],
            normalized_row_count: 300,
            normalized_truncated: true,
            content_hash: 'f'.repeat(64),
            raw_ref: null,
            error_code: null,
            error_message: null,
            supersedes_hash: null,
            origin_job_id: null,
            created_at: '2026-08-08T15:05:00+08:00',
          },
        ],
        count: 1,
        detail: true,
        row_limit: 1,
      },
    });

    const result = await researchApi.listDatasets('600519 / SH', {
      detail: true,
      limit: 1,
      rowLimit: 1,
    });

    expectTypeOf(result).toEqualTypeOf<ResearchDatasetListResponse<true>>();
    expect(get).toHaveBeenCalledWith(
      '/api/v1/research/datasets/600519%20%2F%20SH',
      { params: { detail: true, limit: 1, row_limit: 1 } },
    );
    expect(result.detail).toBe(true);
    expect(result.items[0].normalized).toEqual([
      { trade_date: '20260808', adj_factor: 1.2 },
    ]);
    expect(result.items[0].normalizedTruncated).toBe(true);
  });

  it('keeps dataset detail parameters and dynamic responses type-safe', () => {
    expectTypeOf<ResearchDatasetParams<true>>().toMatchTypeOf<{ detail: true }>();

    const listWithDynamicDetail = (detail: boolean) => (
      researchApi.listDatasets('600519', { detail })
    );
    expectTypeOf(listWithDynamicDetail).returns.toEqualTypeOf<
      Promise<ResearchDatasetListResponse<boolean>>
    >();
  });

  it('omits undefined query parameters and rejects malformed dataset lists', async () => {
    get
      .mockResolvedValueOnce({ data: { items: [], count: 0, detail: false, row_limit: 1000 } })
      .mockResolvedValueOnce({ data: { count: 0, detail: false, row_limit: 1000 } });

    await expect(researchApi.listDatasets('600519')).resolves.toMatchObject({
      items: [],
      count: 0,
    });
    expect(get).toHaveBeenNthCalledWith(1, '/api/v1/research/datasets/600519', {
      params: {},
    });

    await expect(researchApi.listDatasets('600519')).rejects.toThrow(
      'Research dataset list response items must be an array',
    );
  });

  it('lists evidence by durable job with opaque cursor pagination', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [{
          id: 31,
          stock_code: '600519',
          market: 'cn',
          evidence_engine_version: 'evidence-v1',
          claim_policy_version: 'claim-policy-v1',
          as_of: '2026-08-08T08:00:00Z',
          available_at: '2026-08-08T08:00:00Z',
          status: 'partial',
          coverage: 0.5,
          claim_count: 1,
          citation_count: 1,
          input_dataset_hashes: ['a'.repeat(64)],
          factor_snapshot_hash: 'b'.repeat(64),
          evidence_hash: 'c'.repeat(64),
          origin_job_id: 'job-1',
          created_at: '2026-08-08T08:00:00Z',
        }],
        count: 1,
        next_cursor: 'opaque-next-page',
      },
    });

    const result = await researchApi.listEvidence({
      jobId: 'job / 1',
      asOf: '2026-08-08T16:00:00+08:00',
      cursor: 'opaque-current-page',
      limit: 20,
    });

    expect(get).toHaveBeenCalledWith('/api/v1/research/evidence', {
      params: {
        job_id: 'job / 1',
        as_of: '2026-08-08T16:00:00+08:00',
        cursor: 'opaque-current-page',
        limit: 20,
      },
    });
    expect(result.nextCursor).toBe('opaque-next-page');
    expect(result.items[0]).toMatchObject({
      evidenceHash: 'c'.repeat(64),
      claimCount: 1,
      citationCount: 1,
    });
  });

  it('gets typed evidence detail and rejects malformed nested payloads', async () => {
    const detail = {
      id: 31,
      stock_code: '600519',
      market: 'cn',
      evidence_engine_version: 'evidence-v1',
      claim_policy_version: 'claim-policy-v1',
      as_of: '2026-08-08T08:00:00Z',
      available_at: '2026-08-08T08:00:00Z',
      status: 'partial',
      coverage: 0.5,
      claim_count: 1,
      citation_count: 1,
      input_dataset_hashes: ['a'.repeat(64)],
      factor_snapshot_hash: 'b'.repeat(64),
      evidence_hash: 'c'.repeat(64),
      origin_job_id: 'job-1',
      created_at: '2026-08-08T08:00:00Z',
      evidence: {
        evidence_engine_version: 'evidence-v1',
        claim_policy_version: 'claim-policy-v1',
        stock_code: '600519',
        market: 'cn',
        as_of: '2026-08-08T08:00:00Z',
        available_at: '2026-08-08T08:00:00Z',
        status: 'partial',
        coverage: 0.5,
        input_dataset_hashes: ['a'.repeat(64)],
        factor_snapshot_hash: 'b'.repeat(64),
        limitations: [],
        claims: [{
          id: 'claim-1',
          kind: 'factor_metric',
          statement: 'Value is supported.',
          status: 'supported',
          citation_ids: ['citation-1'],
          limitations: [],
          available_at: '2026-08-08T08:00:00Z',
        }],
        citations: [{
          id: 'citation-1',
          relation: 'supports',
          artifact_type: 'factor',
          artifact_hash: 'b'.repeat(64),
          json_pointer: '/factors/value/score',
          value_hash: 'd'.repeat(64),
          available_at: '2026-08-08T08:00:00Z',
          source_name: 'factor_snapshot',
          title: 'Value score',
          excerpt: '72',
          canonical_url: 'https://example.com/value',
        }],
      },
    };
    get
      .mockResolvedValueOnce({ data: detail })
      .mockResolvedValueOnce({ data: { ...detail, evidence: { claims: {} } } });

    const response = await researchApi.getEvidence('hash / evidence');

    expect(get).toHaveBeenNthCalledWith(
      1,
      '/api/v1/research/evidence/hash%20%2F%20evidence',
    );
    expect(response.evidence.claims[0].citationIds).toEqual(['citation-1']);
    expect(response.evidence.citations[0].canonicalUrl).toBe(
      'https://example.com/value',
    );

    await expect(researchApi.getEvidence('malformed')).rejects.toThrow(
      'Research evidence detail response is malformed',
    );
  });
});
