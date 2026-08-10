import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { researchWatchlistApi } from '../api/research';
import { systemConfigApi } from '../api/systemConfig';
import { useUiLanguage } from '../contexts/UiLanguageContext';
import type {
  ResearchHoldingsFreshness,
  ResearchWatchlistItem,
  ResearchWatchlistMarket,
} from '../types/research';
import { areStockCodesEquivalent, findMatchingStockCode, includesStockCode } from '../utils/stockCode';

export interface UseWatchlistReturn {
  watchlistCodes: string[];
  effectiveItems: EffectiveWatchlistItem[];
  effectiveCodes: string[];
  holdingsFreshness: ResearchHoldingsFreshness;
  isLoading: boolean;
  isActioning: boolean;
  actionMessage: string | null;
  isInWatchlist: (stockCode: string) => boolean;
  addToWatchlist: (stockCode: string) => Promise<void>;
  removeFromWatchlist: (stockCode: string) => Promise<void>;
  removeEffectiveItem: (market: ResearchWatchlistMarket, stockCode: string) => Promise<void>;
  toggleWatchlist: (stockCode: string) => Promise<void>;
  refresh: () => Promise<void>;
}

export type LegacyEffectiveWatchlistItem = Omit<ResearchWatchlistItem, 'market' | 'sources'> & {
  market: '';
  sources: ['legacy'];
};

export type EffectiveWatchlistItem = ResearchWatchlistItem | LegacyEffectiveWatchlistItem;

function toLegacyEffectiveItems(codes: string[]): LegacyEffectiveWatchlistItem[] {
  return codes.map((stockCode) => ({
    stockCode,
    market: '',
    sources: ['legacy'],
    reason: null,
    priority: 0,
    analysisTier: 'quick',
    nextReviewAt: null,
    isActive: true,
    isHolding: false,
  }));
}

function loadWatchlistState() {
  return Promise.allSettled([
    systemConfigApi.getWatchlist(),
    researchWatchlistApi.getUniverse(),
  ] as const);
}

export function useWatchlist(): UseWatchlistReturn {
  const { t } = useUiLanguage();
  const [codes, setCodes] = useState<string[]>([]);
  const [effectiveItems, setEffectiveItems] = useState<EffectiveWatchlistItem[]>([]);
  const [holdingsFreshness, setHoldingsFreshness] = useState<ResearchHoldingsFreshness>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [isActioning, setIsActioning] = useState(false);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const messageTimerRef = useRef<number | null>(null);
  const mountedRef = useRef(true);
  const refreshVersionRef = useRef(0);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
      if (messageTimerRef.current !== null) {
        window.clearTimeout(messageTimerRef.current);
      }
    };
  }, []);

  const applyWatchlistState = useCallback((
    refreshVersion: number,
    [legacyResult, universeResult]: Awaited<ReturnType<typeof loadWatchlistState>>,
  ) => {
    if (!mountedRef.current || refreshVersion !== refreshVersionRef.current) {
      return;
    }

    if (legacyResult.status === 'fulfilled') {
      setCodes(legacyResult.value);
    }

    if (universeResult.status === 'fulfilled') {
      setEffectiveItems(universeResult.value.items);
      setHoldingsFreshness(universeResult.value.holdingsFreshness);
    } else if (legacyResult.status === 'fulfilled') {
      // Older backends do not expose the research universe. Preserve the
      // legacy watchlist behavior until that endpoint is available.
      setEffectiveItems(toLegacyEffectiveItems(legacyResult.value));
      setHoldingsFreshness(null);
    }
  }, []);

  const refresh = useCallback(async () => {
    const refreshVersion = refreshVersionRef.current + 1;
    refreshVersionRef.current = refreshVersion;
    const results = await loadWatchlistState();
    applyWatchlistState(refreshVersion, results);
  }, [applyWatchlistState]);

  const refreshEffectiveItems = useCallback(async (fallbackCodes: string[]) => {
    const refreshVersion = refreshVersionRef.current + 1;
    refreshVersionRef.current = refreshVersion;
    try {
      const result = await researchWatchlistApi.getUniverse();
      if (!mountedRef.current || refreshVersion !== refreshVersionRef.current) {
        return;
      }
      setEffectiveItems(result.items);
      setHoldingsFreshness(result.holdingsFreshness);
    } catch {
      if (mountedRef.current && refreshVersion === refreshVersionRef.current) {
        setEffectiveItems(toLegacyEffectiveItems(fallbackCodes));
        setHoldingsFreshness(null);
      }
    }
  }, []);

  const effectiveCodes = useMemo(
    () => effectiveItems.map((item) => item.stockCode),
    [effectiveItems],
  );

  useEffect(() => {
    const refreshVersion = refreshVersionRef.current + 1;
    refreshVersionRef.current = refreshVersion;
    void loadWatchlistState().then((results) => {
      applyWatchlistState(refreshVersion, results);
      if (mountedRef.current) {
        setIsLoading(false);
      }
    });
  }, [applyWatchlistState]);

  const showMessage = useCallback((msg: string) => {
    if (messageTimerRef.current !== null) {
      window.clearTimeout(messageTimerRef.current);
    }
    setActionMessage(msg);
    messageTimerRef.current = window.setTimeout(() => {
      if (mountedRef.current) {
        setActionMessage(null);
      }
    }, 3000);
  }, []);

  const isInWatchlist = useCallback(
    (stockCode: string) => includesStockCode(codes, stockCode),
    [codes],
  );

  const addToWatchlist = useCallback(async (stockCode: string) => {
    if (!stockCode || isActioning) return;
    setIsActioning(true);
    try {
      const result = await systemConfigApi.addToWatchlist(stockCode);
      if (mountedRef.current) {
        setCodes(result);
        showMessage(t('watchlist.actionAdded', { code: stockCode }));
      }
      await refreshEffectiveItems(result);
    } catch {
      if (mountedRef.current) showMessage(t('watchlist.actionFailed'));
    } finally {
      if (mountedRef.current) setIsActioning(false);
    }
  }, [isActioning, refreshEffectiveItems, showMessage, t]);

  const removeFromWatchlist = useCallback(async (stockCode: string) => {
    if (!stockCode || isActioning) return;
    setIsActioning(true);
    try {
      const result = await systemConfigApi.removeFromWatchlist(stockCode);
      if (mountedRef.current) {
        setCodes(result);
        showMessage(t('watchlist.actionRemoved', { code: stockCode }));
      }
      await refreshEffectiveItems(result);
    } catch {
      if (mountedRef.current) showMessage(t('watchlist.actionFailed'));
    } finally {
      if (mountedRef.current) setIsActioning(false);
    }
  }, [isActioning, refreshEffectiveItems, showMessage, t]);

  const removeEffectiveItem = useCallback(async (market: ResearchWatchlistMarket, stockCode: string) => {
    if (!market || !stockCode || isActioning) return;
    setIsActioning(true);
    try {
      await researchWatchlistApi.removeItem(market, stockCode);
      if (mountedRef.current) {
        setCodes((current) => current.filter((code) => !areStockCodesEquivalent(code, stockCode)));
      }
      await refresh();
      if (mountedRef.current) {
        showMessage(t('watchlist.actionRemoved', { code: stockCode }));
      }
    } catch {
      if (mountedRef.current) showMessage(t('watchlist.actionFailed'));
    } finally {
      if (mountedRef.current) setIsActioning(false);
    }
  }, [isActioning, refresh, showMessage, t]);

  const toggleWatchlist = useCallback(async (stockCode: string) => {
    const existingStockCode = findMatchingStockCode(codes, stockCode);
    if (existingStockCode) {
      await removeFromWatchlist(existingStockCode);
    } else {
      await addToWatchlist(stockCode);
    }
  }, [codes, removeFromWatchlist, addToWatchlist]);

  return {
    watchlistCodes: codes,
    effectiveItems,
    effectiveCodes,
    holdingsFreshness,
    isLoading,
    isActioning,
    actionMessage,
    isInWatchlist,
    addToWatchlist,
    removeFromWatchlist,
    removeEffectiveItem,
    toggleWatchlist,
    refresh,
  };
}
