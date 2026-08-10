import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { portfolioApi } from '../../../api/portfolio';
import { UiLanguageProvider } from '../../../contexts/UiLanguageContext';
import { UI_LANGUAGE_STORAGE_KEY } from '../../../utils/uiLanguage';
import { PortfolioReconciliationPanel } from '../PortfolioReconciliationPanel';

const {
  listReconciliations,
  previewReconciliation,
  applyReconciliation,
} = vi.hoisted(() => ({
  listReconciliations: vi.fn(),
  previewReconciliation: vi.fn(),
  applyReconciliation: vi.fn(),
}));

vi.mock('../../../api/portfolio', () => ({
  portfolioApi: {
    listReconciliations,
    previewReconciliation,
    applyReconciliation,
  },
}));

const account = {
  id: 7,
  name: 'Broker account',
  broker: 'demo',
  market: 'us' as const,
  baseCurrency: 'USD',
  isActive: true,
};

function renderPanel(onApplied = vi.fn()) {
  window.localStorage.setItem(UI_LANGUAGE_STORAGE_KEY, 'en');
  render(
    <UiLanguageProvider>
      <PortfolioReconciliationPanel accounts={[account]} onApplied={onApplied} />
    </UiLanguageProvider>,
  );
  return onApplied;
}

describe('PortfolioReconciliationPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();
    listReconciliations.mockResolvedValue({ items: [] });
    previewReconciliation.mockResolvedValue({
      id: 11,
      previewToken: 'opaque-preview-token',
      eventType: 'opening',
      effectiveDate: '2026-08-10',
      expiresAt: '2026-08-10T00:15:00',
      inputHash: 'a'.repeat(64),
      bookHash: 'b'.repeat(64),
      targetHash: 'c'.repeat(64),
      diff: { adjustments: [{ adjustmentType: 'cash' }] },
      warnings: ['FIFO baseline will be reset.'],
    });
    applyReconciliation.mockResolvedValue({
      id: 11,
      accountId: 7,
      eventType: 'opening',
      status: 'applied',
      eventVersion: 1,
      effectiveDate: '2026-08-10',
      inputHash: 'a'.repeat(64),
    });
  });

  it('submits absolute targets, then applies only the opaque preview token', async () => {
    const onApplied = renderPanel();
    await waitFor(() => expect(listReconciliations).toHaveBeenCalledWith(7, false));

    fireEvent.change(screen.getByLabelText('Event type'), { target: { value: 'opening' } });
    fireEvent.change(screen.getByLabelText('Effective date'), { target: { value: '2026-08-10' } });
    fireEvent.change(screen.getByLabelText('Absolute cash targets (JSON)'), {
      target: { value: '[{"currency":"USD","balance":1200}]' },
    });
    fireEvent.change(screen.getByLabelText('Absolute position targets (JSON)'), {
      target: {
        value: '[{"stockCode":"AAPL","market":"us","currency":"USD","quantity":3,"totalCost":450}]',
      },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Preview' }));

    await waitFor(() => expect(previewReconciliation).toHaveBeenCalledWith(7, {
      eventType: 'opening',
      effectiveDate: '2026-08-10',
      cash: [{ currency: 'USD', balance: 1200 }],
      positions: [{ stockCode: 'AAPL', market: 'us', currency: 'USD', quantity: 3, totalCost: 450 }],
      source: 'broker_statement',
      note: null,
    }));
    expect(await screen.findByText('FIFO baseline will be reset.')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Apply preview' }));
    await waitFor(() => expect(applyReconciliation).toHaveBeenCalledTimes(1));
    expect(applyReconciliation).toHaveBeenCalledWith(7, {
      previewToken: 'opaque-preview-token',
      idempotencyKey: expect.any(String),
    });
    expect(onApplied).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(listReconciliations).toHaveBeenCalledTimes(2));
  });

  it('rejects a non-array target before calling the preview API', async () => {
    renderPanel();
    await waitFor(() => expect(listReconciliations).toHaveBeenCalledTimes(1));
    fireEvent.change(screen.getByLabelText('Absolute position targets (JSON)'), {
      target: { value: '{"stockCode":"AAPL"}' },
    });
    fireEvent.click(screen.getByRole('button', { name: 'Preview' }));

    expect(await screen.findByText('Absolute position targets (JSON) must be a JSON array')).toBeInTheDocument();
    expect(portfolioApi.previewReconciliation).not.toHaveBeenCalled();
  });

  it('reuses the preview-scoped idempotency key after a lost apply response', async () => {
    applyReconciliation
      .mockRejectedValueOnce(new Error('connection lost after apply'))
      .mockResolvedValueOnce({
        id: 11,
        accountId: 7,
        eventType: 'opening',
        status: 'applied',
        eventVersion: 1,
        effectiveDate: '2026-08-10',
        inputHash: 'a'.repeat(64),
        source: 'broker_statement',
        warnings: [],
      });
    renderPanel();
    await waitFor(() => expect(listReconciliations).toHaveBeenCalledTimes(1));
    fireEvent.click(screen.getByRole('button', { name: 'Preview' }));
    expect(await screen.findByRole('button', { name: 'Apply preview' })).toBeEnabled();

    fireEvent.click(screen.getByRole('button', { name: 'Apply preview' }));
    await waitFor(() => expect(applyReconciliation).toHaveBeenCalledTimes(1));
    const firstKey = applyReconciliation.mock.calls[0][1].idempotencyKey;
    await waitFor(() => expect(screen.getByRole('button', { name: 'Apply preview' })).toBeEnabled());
    fireEvent.click(screen.getByRole('button', { name: 'Apply preview' }));
    await waitFor(() => expect(applyReconciliation).toHaveBeenCalledTimes(2));

    expect(applyReconciliation.mock.calls[1][1].idempotencyKey).toBe(firstKey);
  });
});
