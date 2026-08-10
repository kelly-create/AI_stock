import { act, renderHook, waitFor } from '@testing-library/react';
import type { PropsWithChildren } from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { UiLanguageProvider } from '../../contexts/UiLanguageContext';
import { UI_LANGUAGE_STORAGE_KEY } from '../../utils/uiLanguage';
import { useWatchlist } from '../useWatchlist';

const {
  mockGetWatchlist,
  mockAddToWatchlist,
  mockRemoveFromWatchlist,
  mockGetUniverse,
  mockRemoveResearchItem,
} = vi.hoisted(() => ({
  mockGetWatchlist: vi.fn(),
  mockAddToWatchlist: vi.fn(),
  mockRemoveFromWatchlist: vi.fn(),
  mockGetUniverse: vi.fn(),
  mockRemoveResearchItem: vi.fn(),
}));

vi.mock('../../api/research', () => ({
  researchWatchlistApi: {
    getUniverse: mockGetUniverse,
    removeItem: mockRemoveResearchItem,
  },
}));

vi.mock('../../api/systemConfig', () => ({
  systemConfigApi: {
    getWatchlist: mockGetWatchlist,
    addToWatchlist: mockAddToWatchlist,
    removeFromWatchlist: mockRemoveFromWatchlist,
  },
}));

describe('useWatchlist', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();
    mockGetWatchlist.mockResolvedValue([]);
    mockAddToWatchlist.mockResolvedValue([]);
    mockRemoveFromWatchlist.mockResolvedValue([]);
    mockGetUniverse.mockRejectedValue(new Error('research universe unavailable'));
    mockRemoveResearchItem.mockResolvedValue({ deleted: 1 });
  });

  it('matches raw HK watchlist entries against prefixed and suffixed variants', async () => {
    mockGetWatchlist.mockResolvedValue(['00700']);

    const { result } = renderHook(() => useWatchlist());

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.isInWatchlist('00700')).toBe(true);
    expect(result.current.isInWatchlist('HK00700')).toBe(true);
    expect(result.current.isInWatchlist('00700.HK')).toBe(true);
    expect(result.current.isInWatchlist('HK01810')).toBe(false);
  });

  it('removes the matched raw watchlist entry instead of adding a duplicate variant', async () => {
    mockGetWatchlist.mockResolvedValue(['00700']);
    mockRemoveFromWatchlist.mockResolvedValue([]);

    const { result } = renderHook(() => useWatchlist());

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    await act(async () => {
      await result.current.toggleWatchlist('HK00700');
    });

    expect(mockRemoveFromWatchlist).toHaveBeenCalledWith('00700');
    expect(mockAddToWatchlist).not.toHaveBeenCalled();
    await waitFor(() => {
      expect(result.current.watchlistCodes).toEqual([]);
    });
  });

  it('compares submitted and stored US tickers case-insensitively', async () => {
    mockGetWatchlist.mockResolvedValue(['aapl']);
    mockRemoveFromWatchlist.mockResolvedValue([]);

    const { result } = renderHook(() => useWatchlist());

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.isInWatchlist('AAPL')).toBe(true);

    await act(async () => {
      await result.current.toggleWatchlist('AAPL');
    });

    expect(mockRemoveFromWatchlist).toHaveBeenCalledWith('aapl');
    expect(mockAddToWatchlist).not.toHaveBeenCalled();
  });

  it('falls back to legacy codes when the research universe is unavailable', async () => {
    mockGetWatchlist.mockResolvedValue(['600519', 'AAPL']);

    const { result } = renderHook(() => useWatchlist());

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.effectiveCodes).toEqual(['600519', 'AAPL']);
    expect(result.current.effectiveItems).toEqual([
      expect.objectContaining({ stockCode: '600519', market: '', sources: ['legacy'] }),
      expect.objectContaining({ stockCode: 'AAPL', market: '', sources: ['legacy'] }),
    ]);
    expect(result.current.holdingsFreshness).toBeNull();
  });

  it('keeps the legacy membership contract separate from the effective universe', async () => {
    mockGetWatchlist.mockResolvedValue(['600519']);
    mockGetUniverse.mockResolvedValue({
      items: [
        {
          stockCode: '600519',
          market: 'cn',
          sources: ['legacy'],
          reason: null,
          priority: 0,
          analysisTier: 'quick',
          nextReviewAt: null,
          isActive: true,
          isHolding: false,
        },
        {
          stockCode: 'AAPL',
          market: 'us',
          sources: ['holding'],
          reason: null,
          priority: 40,
          analysisTier: 'standard',
          nextReviewAt: '2026-08-12T09:30:00+08:00',
          isActive: false,
          isHolding: true,
        },
      ],
      holdingsFreshness: 'ledger',
    });

    const { result } = renderHook(() => useWatchlist());

    await waitFor(() => {
      expect(result.current.isLoading).toBe(false);
    });

    expect(result.current.watchlistCodes).toEqual(['600519']);
    expect(result.current.effectiveCodes).toEqual(['600519', 'AAPL']);
    expect(result.current.isInWatchlist('AAPL')).toBe(false);
    expect(result.current.holdingsFreshness).toBe('ledger');
  });

  it('removes an effective item only through the research API and refreshes both views', async () => {
    mockGetWatchlist
      .mockResolvedValueOnce(['600519'])
      .mockResolvedValueOnce([]);
    mockGetUniverse
      .mockResolvedValueOnce({
        items: [{
          stockCode: '600519',
          market: 'cn',
          sources: ['enhanced'],
          reason: 'quality',
          priority: 80,
          analysisTier: 'deep',
          nextReviewAt: null,
          isActive: true,
          isHolding: false,
        }],
        holdingsFreshness: 'ledger',
      })
      .mockResolvedValueOnce({ items: [], holdingsFreshness: 'ledger' });

    const { result } = renderHook(() => useWatchlist());
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await act(async () => {
      await result.current.removeEffectiveItem('cn', '600519');
    });

    expect(mockRemoveResearchItem).toHaveBeenCalledWith('cn', '600519');
    expect(mockRemoveFromWatchlist).not.toHaveBeenCalled();
    expect(result.current.watchlistCodes).toEqual([]);
    expect(result.current.effectiveCodes).toEqual([]);
  });

  it('localizes action feedback without changing the legacy add contract', async () => {
    window.localStorage.setItem(UI_LANGUAGE_STORAGE_KEY, 'en');
    mockAddToWatchlist.mockResolvedValue(['AAPL']);
    const wrapper = ({ children }: PropsWithChildren) => (
      <UiLanguageProvider>{children}</UiLanguageProvider>
    );

    const { result } = renderHook(() => useWatchlist(), { wrapper });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await act(async () => {
      await result.current.addToWatchlist('AAPL');
    });

    expect(mockAddToWatchlist).toHaveBeenCalledWith('AAPL');
    expect(result.current.actionMessage).toBe('Added AAPL to the watchlist');
  });

  it('keeps the latest refresh result and still settles initial loading', async () => {
    let resolveInitialLegacy!: (codes: string[]) => void;
    const initialLegacy = new Promise<string[]>((resolve) => {
      resolveInitialLegacy = resolve;
    });
    mockGetWatchlist
      .mockReturnValueOnce(initialLegacy)
      .mockResolvedValueOnce(['AAPL']);

    const { result } = renderHook(() => useWatchlist());

    await act(async () => {
      await result.current.refresh();
    });
    expect(result.current.effectiveCodes).toEqual(['AAPL']);
    expect(result.current.isLoading).toBe(true);

    await act(async () => {
      resolveInitialLegacy(['600519']);
      await initialLegacy;
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(result.current.watchlistCodes).toEqual(['AAPL']);
    expect(result.current.effectiveCodes).toEqual(['AAPL']);
  });
});
