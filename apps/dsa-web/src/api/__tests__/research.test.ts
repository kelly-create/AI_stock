import { beforeEach, describe, expect, expectTypeOf, it, vi } from 'vitest';
import { researchApi, researchWatchlistApi } from '../research';
import type {
  ResearchDatasetListResponse,
  ResearchDatasetParams,
  ResearchUniverseResponse,
  ResearchWatchlistResponse,
} from '../../types/research';

const { get, put, deleteRequest } = vi.hoisted(() => ({
  get: vi.fn(),
  put: vi.fn(),
  deleteRequest: vi.fn(),
}));

vi.mock('../index', () => ({
  default: { get, put, delete: deleteRequest },
}));

describe('researchApi', () => {
  beforeEach(() => {
    get.mockReset();
    put.mockReset();
    deleteRequest.mockReset();
  });

  it('gets the enhanced watchlist and converts metadata to camel case', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [{
          stock_code: '600519',
          market: 'cn',
          sources: ['enhanced'],
          reason: 'quality compounder',
          priority: 80,
          analysis_tier: 'deep',
          next_review_at: '2026-08-12T09:30:00+08:00',
          is_active: true,
          is_holding: false,
        }],
        holdings_freshness: 'ledger',
      },
    });

    const result = await researchWatchlistApi.getWatchlist();

    expectTypeOf(result).toEqualTypeOf<ResearchWatchlistResponse>();
    expect(get).toHaveBeenCalledWith('/api/v1/research/watchlist');
    expect(result).toEqual({
      items: [{
        stockCode: '600519',
        market: 'cn',
        sources: ['enhanced'],
        reason: 'quality compounder',
        priority: 80,
        analysisTier: 'deep',
        nextReviewAt: '2026-08-12T09:30:00+08:00',
        isActive: true,
        isHolding: false,
      }],
      holdingsFreshness: 'ledger',
    });
  });

  it('gets the effective research universe including holding provenance', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [{
          stock_code: '00700',
          market: 'hk',
          sources: ['legacy', 'holding'],
          reason: null,
          priority: 0,
          analysis_tier: 'quick',
          next_review_at: null,
          is_active: true,
          is_holding: true,
        }],
        holdings_freshness: 'ledger',
      },
    });

    const result = await researchWatchlistApi.getUniverse();

    expectTypeOf(result).toEqualTypeOf<ResearchUniverseResponse>();
    expect(get).toHaveBeenCalledWith('/api/v1/research/universe');
    expect(result.items[0]).toMatchObject({
      stockCode: '00700',
      market: 'hk',
      sources: ['legacy', 'holding'],
      isHolding: true,
      nextReviewAt: null,
    });
    expect(result.holdingsFreshness).toBe('ledger');
  });

  it('replaces complete watchlist metadata using snake-case fields', async () => {
    put.mockResolvedValueOnce({
      data: {
        stock_code: 'BRK.B',
        market: 'us',
        sources: ['enhanced'],
        reason: null,
        priority: 65,
        analysis_tier: 'standard',
        next_review_at: null,
        is_active: true,
        is_holding: false,
      },
    });

    const result = await researchWatchlistApi.upsertItem(
      'us',
      'BRK.B',
      {
        reason: null,
        priority: 65,
        analysisTier: 'standard',
        nextReviewAt: null,
      },
    );

    expect(put).toHaveBeenCalledWith(
      '/api/v1/research/watchlist/us/BRK.B',
      {
        reason: null,
        priority: 65,
        analysis_tier: 'standard',
        next_review_at: null,
      },
    );
    expect(result).toMatchObject({
      stockCode: 'BRK.B',
      analysisTier: 'standard',
      nextReviewAt: null,
    });
  });

  it('removes an item by identity and requires body-level deletion confirmation', async () => {
    deleteRequest.mockResolvedValueOnce({ data: { deleted: 1 } });

    const removed = await researchWatchlistApi.removeItem(
      'cn',
      '600519.SH',
    );
    expect(deleteRequest).toHaveBeenCalledWith(
      '/api/v1/research/watchlist/cn/600519.SH',
    );
    expect(removed.deleted).toBe(1);

    deleteRequest.mockResolvedValueOnce({ data: { deleted: 0 } });
    await expect(
      researchWatchlistApi.removeItem('cn', '600519'),
    ).rejects.toThrow('Research watchlist delete response did not confirm deletion');
  });

  it('rejects malformed watchlist responses and item provenance', async () => {
    get
      .mockResolvedValueOnce({ data: { holdings_freshness: null } })
      .mockResolvedValueOnce({
        data: {
          items: [{ stock_code: '600519', sources: null }],
          holdings_freshness: null,
        },
      })
      .mockResolvedValueOnce({
        data: {
          items: [{ stock_code: '600519', sources: ['portfolio'] }],
          holdings_freshness: 'ledger',
        },
      })
      .mockResolvedValueOnce({
        data: {
          items: [],
          holdings_freshness: 'cached',
        },
      });

    await expect(researchWatchlistApi.getWatchlist()).rejects.toThrow(
      'Research watchlist response items must be an array',
    );
    await expect(researchWatchlistApi.getUniverse()).rejects.toThrow(
      'Research watchlist item sources must be an array',
    );
    await expect(researchWatchlistApi.getUniverse()).rejects.toThrow(
      'Research watchlist item contains an unknown source',
    );
    await expect(researchWatchlistApi.getUniverse()).rejects.toThrow(
      'Research watchlist response must confirm ledger freshness',
    );
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
        debate_snapshot_hash: 'f'.repeat(64),
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
    expect(result.debateSnapshotHash).toBe('f'.repeat(64));
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

  it('lists debates by durable job with evidence filtering and opaque pagination', async () => {
    get.mockResolvedValueOnce({
      data: {
        items: [{
          id: 41,
          stock_code: '600519',
          market: 'cn',
          debate_engine_version: 'research-debate-v1',
          output_schema_version: 'research-debate-output-v1',
          prompt_version: 'research-debate-prompt-v1',
          evidence_snapshot_hash: 'a'.repeat(64),
          request_hash: 'b'.repeat(64),
          model_route_fingerprint: 'c'.repeat(64),
          as_of: '2026-08-08T08:00:00Z',
          available_at: '2026-08-08T08:00:00Z',
          status: 'available',
          bull_turn_hash: 'd'.repeat(64),
          bear_turn_hash: 'e'.repeat(64),
          bull_argument_count: 2,
          bear_argument_count: 1,
          open_question_count: 1,
          debate_hash: 'f'.repeat(64),
          origin_job_id: 'job-1',
          created_at: '2026-08-08T08:00:00Z',
        }],
        count: 1,
        next_cursor: 'opaque-debate-next',
      },
    });

    const result = await researchApi.listDebates({
      jobId: 'job / 1',
      evidenceSnapshotHash: 'a'.repeat(64),
      asOf: '2026-08-08T16:00:00+08:00',
      cursor: 'opaque-debate-current',
      limit: 20,
    });

    expect(get).toHaveBeenCalledWith('/api/v1/research/debates', {
      params: {
        job_id: 'job / 1',
        evidence_snapshot_hash: 'a'.repeat(64),
        as_of: '2026-08-08T16:00:00+08:00',
        cursor: 'opaque-debate-current',
        limit: 20,
      },
    });
    expect(result.nextCursor).toBe('opaque-debate-next');
    expect(result.items[0]).toMatchObject({
      debateHash: 'f'.repeat(64),
      bullArgumentCount: 2,
      bearArgumentCount: 1,
      openQuestionCount: 1,
    });
  });

  it('gets typed debate detail and rejects malformed nested payloads', async () => {
    const summary = {
      id: 41,
      stock_code: '600519',
      market: 'cn',
      debate_engine_version: 'research-debate-v1',
      output_schema_version: 'research-debate-output-v1',
      prompt_version: 'research-debate-prompt-v1',
      evidence_snapshot_hash: 'a'.repeat(64),
      request_hash: 'b'.repeat(64),
      model_route_fingerprint: 'c'.repeat(64),
      as_of: '2026-08-08T08:00:00Z',
      available_at: '2026-08-08T08:00:00Z',
      status: 'available',
      bull_turn_hash: 'd'.repeat(64),
      bear_turn_hash: 'e'.repeat(64),
      bull_argument_count: 1,
      bear_argument_count: 0,
      open_question_count: 1,
      debate_hash: 'f'.repeat(64),
      origin_job_id: 'job-1',
      created_at: '2026-08-08T08:00:00Z',
    };
    const detail = {
      ...summary,
      debate: {
        debate_engine_version: 'research-debate-v1',
        output_schema_version: 'research-debate-output-v1',
        prompt_version: 'research-debate-prompt-v1',
        stock_code: '600519',
        market: 'cn',
        as_of: '2026-08-08T08:00:00Z',
        available_at: '2026-08-08T08:00:00Z',
        status: 'available',
        evidence_snapshot_hash: 'a'.repeat(64),
        request_hash: 'b'.repeat(64),
        model_route_fingerprint: 'c'.repeat(64),
        bull_turn_hash: 'd'.repeat(64),
        bear_turn_hash: 'e'.repeat(64),
        failed_stances: [],
        limitations: [],
        turns: [{
          turn_hash: 'd'.repeat(64),
          prompt_fingerprint: '1'.repeat(64),
          model_used: 'provider/model',
          stance: 'bull',
          summary: 'A bounded Bull view.',
          arguments: [{
            id: 'bull-1',
            statement: 'Value evidence is supportive.',
            claim_ids: ['claim-1'],
            citation_ids: ['citation-1'],
            confidence: 0.7,
            limitations: [],
          }],
          open_questions: ['Will the valuation gap persist?'],
        }],
      },
    };
    get
      .mockResolvedValueOnce({ data: detail })
      .mockResolvedValueOnce({
        data: { ...detail, debate: { ...detail.debate, turns: [{ arguments: {} }] } },
      })
      .mockResolvedValueOnce({
        data: { ...detail, debate: { ...detail.debate, failed_stances: ['bear'] } },
      });

    const response = await researchApi.getDebate('hash / debate');

    expect(get).toHaveBeenNthCalledWith(
      1,
      '/api/v1/research/debates/hash%20%2F%20debate',
    );
    expect(response.debate.turns[0].arguments[0].claimIds).toEqual(['claim-1']);
    expect(response.debate.turns[0].openQuestions).toEqual([
      'Will the valuation gap persist?',
    ]);

    await expect(researchApi.getDebate('malformed')).rejects.toThrow(
      'Research debate detail response is malformed',
    );
    await expect(researchApi.getDebate('malformed-failure')).rejects.toThrow(
      'Research debate detail response is malformed',
    );
  });

  it('reads a formal Personal Research Thesis and follows its immutable lineage', async () => {
    const skillIds = [
      'personal-value-quality',
      'personal-trend-timing',
      'personal-catalyst',
      'personal-risk',
      'personal-evidence-quality',
    ];
    const skillHashes = Object.fromEntries(
      skillIds.map((skillId, index) => [skillId, String(index + 1).repeat(64)]),
    );
    const thesis = {
      contract: 'personal-research-thesis',
      version: 'personal-research-thesis-v1',
      thesis_hash: 'a'.repeat(64),
      content_hash: 'b'.repeat(64),
      lineage: {
        task_id: 'task / 1',
        market: 'cn',
        stock_code: '600519',
        research_snapshot_hash: 'c'.repeat(64),
        skill_execution_hashes: skillHashes,
        debate_snapshot_hash: 'd'.repeat(64),
        debate_review_hash: 'e'.repeat(64),
        decision_signal_id: 42,
        policy_evaluation_hash: null,
        policy_version: null,
        policy_hash: null,
        portfolio_snapshot_ref: null,
        supersedes_thesis_hash: null,
      },
      stance: 'bullish',
      account_action: 'open_candidate',
      scores: {
        value_quality_score: 70,
        trend_timing_score: 71,
        catalyst_score: 72,
        risk_score: 73,
        evidence_quality_score: 74,
      },
      catalysts: ['earnings'],
      invalidators: ['margin decline'],
      unknowns: ['guidance'],
      evidence_refs: ['citation-1'],
      content: { contract: 'formal-thesis-content-v1' },
      created_at: '2026-08-10T08:00:00Z',
    };
    const execution = {
      contract: 'personal-research-skill-execution',
      version: 'v1',
      execution_hash: skillHashes['personal-value-quality'],
      skill_contract: {
        skill_id: 'personal-value-quality',
        version: 'value-quality-v1',
        contract_hash: 'f'.repeat(64),
        score_field: 'value_quality_score',
      },
      lineage: {
        task_id: 'task / 1',
        market: 'cn',
        stock_code: '600519',
        research_snapshot_hash: 'c'.repeat(64),
        factor_snapshot_hash: '1'.repeat(64),
        evidence_snapshot_hash: '2'.repeat(64),
        dataset_snapshot_hashes: ['3'.repeat(64)],
        dataset_lineage_hash: '4'.repeat(64),
        input_hash: '5'.repeat(64),
        output_hash: '6'.repeat(64),
      },
      input: { contract: 'input-v1' },
      result: { status: 'succeeded', score: 70, output: { evidence_refs: ['citation-1'] } },
      created_at: '2026-08-10T08:00:00Z',
    };
    const executionList = {
      contract: 'personal-research-skill-execution-collection',
      version: 'v1',
      lineage: { task_id: 'task / 1', market: 'cn', stock_code: '600519' },
      expected_skill_ids: skillIds,
      missing_skill_ids: skillIds.slice(1),
      complete: false,
      executions: [execution],
    };
    const review = {
      contract: 'personal-research-debate-review',
      version: 'v1',
      review_hash: 'e'.repeat(64),
      lineage: {
        task_id: 'task / 1',
        market: 'cn',
        stock_code: '600519',
        debate_snapshot_hash: 'd'.repeat(64),
        evidence_snapshot_hash: '2'.repeat(64),
      },
      verifier: {
        version: 'verifier-v1',
        input_hash: '1'.repeat(64),
        output_hash: '2'.repeat(64),
        valid: true,
        fail_closed: false,
        reason_codes: [],
        input: {},
        output: {},
      },
      judge: {
        version: 'judge-v1',
        policy_hash: '3'.repeat(64),
        input_hash: '4'.repeat(64),
        output_hash: '5'.repeat(64),
        fail_closed: false,
        reason_codes: ['balanced_evidence'],
        verdict: 'balanced',
        winner: null,
        input: {},
        output: {},
      },
      created_at: '2026-08-10T08:00:00Z',
    };
    get
      .mockResolvedValueOnce({ data: thesis })
      .mockResolvedValueOnce({ data: executionList })
      .mockResolvedValueOnce({ data: review });

    const thesisResult = await researchApi.getLatestPersonalResearchThesisBySignal(42);
    const skillsResult = await researchApi.listPersonalResearchSkillExecutions(
      thesisResult.lineage.taskId,
      thesisResult.lineage.market,
      thesisResult.lineage.stockCode,
    );
    const reviewResult = await researchApi.getPersonalResearchDebateReview(
      thesisResult.lineage.debateReviewHash!,
    );

    expect(get).toHaveBeenNthCalledWith(
      1,
      '/api/v1/research/personal/artifacts/theses/by-signal/42',
    );
    expect(get).toHaveBeenNthCalledWith(
      2,
      '/api/v1/research/personal/artifacts/skills/tasks/task%20%2F%201/stocks/cn/600519',
    );
    expect(get).toHaveBeenNthCalledWith(
      3,
      `/api/v1/research/personal/artifacts/debate-reviews/${'e'.repeat(64)}`,
    );
    expect(thesisResult.lineage.skillExecutionHashes['personal-value-quality'])
      .toBe('1'.repeat(64));
    expect(skillsResult.executions[0].skillContract.version).toBe('value-quality-v1');
    expect(reviewResult.verifier.valid).toBe(true);
    expect(reviewResult.judge.reasonCodes).toEqual(['balanced_evidence']);
  });

  it('rejects malformed Personal Research artifact lineage instead of inventing values', async () => {
    get
      .mockResolvedValueOnce({
        data: {
          contract: 'personal-research-thesis',
          version: 'v1',
          lineage: { skill_execution_hashes: {} },
          catalysts: [],
          invalidators: [],
          unknowns: [],
          evidence_refs: [],
          scores: {},
          content: {},
        },
      })
      .mockResolvedValueOnce({
        data: {
          contract: 'personal-research-skill-execution-collection',
          version: 'v1',
          expected_skill_ids: [],
          missing_skill_ids: [],
          executions: {},
        },
      });

    await expect(researchApi.getLatestPersonalResearchThesisBySignal(42)).rejects.toThrow(
      'Personal Research Thesis response is malformed',
    );
    await expect(
      researchApi.listPersonalResearchSkillExecutions('task-1', 'cn', '600519'),
    ).rejects.toThrow('Personal Research Skill execution collection is malformed');
  });
});
