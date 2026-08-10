import { beforeEach, describe, expect, it, vi } from 'vitest';
import { portfolioApi } from '../portfolio';

const { get, post } = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn(),
}));

vi.mock('../index', () => ({
  default: { get, post },
}));

describe('portfolio reconciliation API', () => {
  beforeEach(() => {
    get.mockReset();
    post.mockReset();
  });

  it('serializes absolute targets and converts preview response', async () => {
    post.mockResolvedValueOnce({
      data: {
        id: 9,
        preview_token: 'opaque-token',
        event_type: 'opening',
        effective_date: '2026-08-01',
        expires_at: '2026-08-01T00:15:00',
        input_hash: 'a'.repeat(64),
        book_hash: 'b'.repeat(64),
        target_hash: 'c'.repeat(64),
        diff: { adjustments: [] },
        warnings: [],
      },
    });

    const result = await portfolioApi.previewReconciliation(3, {
      eventType: 'opening',
      effectiveDate: '2026-08-01',
      cash: [{ currency: 'CNY', balance: 1000 }],
      positions: [{
        stockCode: '600519',
        market: 'cn',
        currency: 'CNY',
        quantity: 10,
        totalCost: 900,
      }],
      source: 'broker_statement',
      note: null,
    });

    expect(post).toHaveBeenCalledWith(
      '/api/v1/portfolio/accounts/3/reconciliations/preview',
      {
        event_type: 'opening',
        effective_date: '2026-08-01',
        cash: [{ currency: 'CNY', balance: 1000 }],
        positions: [{
          stock_code: '600519',
          market: 'cn',
          currency: 'CNY',
          quantity: 10,
          total_cost: 900,
        }],
        source: 'broker_statement',
        note: null,
      },
    );
    expect(result.previewToken).toBe('opaque-token');
    expect(result.bookHash).toBe('b'.repeat(64));
  });

  it('applies by opaque token plus idempotency key and lists applied history', async () => {
    post.mockResolvedValueOnce({ data: { id: 9, account_id: 3, event_type: 'opening', status: 'applied', event_version: 1, effective_date: '2026-08-01', input_hash: 'a'.repeat(64) } });
    get.mockResolvedValueOnce({ data: { items: [{ id: 9, account_id: 3, event_type: 'opening', status: 'applied', event_version: 1, effective_date: '2026-08-01', input_hash: 'a'.repeat(64) }] } });

    const applied = await portfolioApi.applyReconciliation(3, {
      previewToken: 'opaque-token',
      idempotencyKey: 'opening-1',
    });
    const history = await portfolioApi.listReconciliations(3);

    expect(post).toHaveBeenCalledWith(
      '/api/v1/portfolio/accounts/3/reconciliations/apply',
      { preview_token: 'opaque-token', idempotency_key: 'opening-1' },
    );
    expect(get).toHaveBeenCalledWith(
      '/api/v1/portfolio/accounts/3/reconciliations',
      { params: { include_previews: false } },
    );
    expect(applied.eventVersion).toBe(1);
    expect(history.items[0].accountId).toBe(3);
  });

  it('keeps the formal primary action and Policy verdict in risk summaries', async () => {
    get.mockResolvedValueOnce({
      data: {
        as_of: '2026-08-01',
        account_id: 3,
        cost_method: 'fifo',
        currency: 'CNY',
        thresholds: {},
        concentration: {},
        sector_concentration: {},
        drawdown: {},
        stop_loss: {},
        decision_signal_risk: {
          available: true,
          total: 1,
          actions: { sell: 0, reduce: 0, alert: 0 },
          items: [{
            account_id: 3,
            symbol: '600519',
            market: 'cn',
            signal: {
              id: 43,
              action: 'buy',
              account_action: 'observe',
              primary_action: 'watch',
              primary_action_source: 'account_action',
              policy_mode: 'enforce',
              policy_decision: 'block',
              would_block: true,
            },
          }],
        },
      },
    });

    const result = await portfolioApi.getRisk({ accountId: 3, asOf: '2026-08-01' });
    const signal = result.decisionSignalRisk?.items[0].signal;

    expect(signal?.action).toBe('buy');
    expect(signal?.accountAction).toBe('observe');
    expect(signal?.primaryAction).toBe('watch');
    expect(signal?.primaryActionSource).toBe('account_action');
    expect(signal?.policyDecision).toBe('block');
    expect(signal?.wouldBlock).toBe(true);
  });
});
