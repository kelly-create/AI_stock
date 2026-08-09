import { useCallback, useEffect, useRef, useState } from 'react';
import { researchApi } from '../api/research';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import type {
  ResearchDebateDetailResponse,
  ResearchDebateSummary,
} from '../types/research';

const DEBATE_PAGE_SIZE = 20;

type DebatePageState = {
  requestKey: string;
  items: ResearchDebateSummary[];
  nextCursor: string | null;
  error: ParsedApiError | null;
  loadMoreError: ParsedApiError | null;
};

export type ResearchDebateDetailState = {
  detail: ResearchDebateDetailResponse | null;
  isLoading: boolean;
  error: ParsedApiError | null;
};

interface UseResearchDebatesOptions {
  taskId?: string | null;
  enabled?: boolean;
}

interface UseResearchDebatesResult {
  items: ResearchDebateSummary[];
  nextCursor: string | null;
  isLoading: boolean;
  isLoadingMore: boolean;
  error: ParsedApiError | null;
  loadMoreError: ParsedApiError | null;
  details: Record<string, ResearchDebateDetailState>;
  refetch: () => void;
  loadMore: () => Promise<void>;
  loadDetail: (debateHash: string, force?: boolean) => Promise<void>;
}

const EMPTY_DETAIL: ResearchDebateDetailState = {
  detail: null,
  isLoading: false,
  error: null,
};

const mergeDebates = (
  current: ResearchDebateSummary[],
  incoming: ResearchDebateSummary[],
): ResearchDebateSummary[] => {
  const byHash = new Map(current.map((item) => [item.debateHash, item]));
  incoming.forEach((item) => byHash.set(item.debateHash, item));
  return Array.from(byHash.values());
};

export function useResearchDebates({
  taskId,
  enabled = true,
}: UseResearchDebatesOptions): UseResearchDebatesResult {
  const normalizedTaskId = taskId?.trim() || '';
  const sourceKey = normalizedTaskId ? `task:${normalizedTaskId}` : 'none';
  const [reloadToken, setReloadToken] = useState(0);
  const requestKey = `${sourceKey}:${reloadToken}`;
  const shouldLoad = enabled && Boolean(normalizedTaskId);
  const [pageState, setPageState] = useState<DebatePageState>({
    requestKey: 'none',
    items: [],
    nextCursor: null,
    error: null,
    loadMoreError: null,
  });
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [details, setDetails] = useState<Record<string, ResearchDebateDetailState>>({});
  const activeRequestKeyRef = useRef(requestKey);
  const loadMoreRequestRef = useRef(0);
  const detailRequestRef = useRef<Record<string, number>>({});
  activeRequestKeyRef.current = requestKey;

  const refetch = useCallback(() => {
    setReloadToken((value) => value + 1);
  }, []);

  useEffect(() => {
    loadMoreRequestRef.current += 1;
    detailRequestRef.current = {};
    setIsLoadingMore(false);
    setDetails({});
  }, [requestKey]);

  useEffect(() => {
    if (!shouldLoad) {
      return undefined;
    }

    let active = true;
    researchApi.listDebates({
      jobId: normalizedTaskId,
      limit: DEBATE_PAGE_SIZE,
    }).then((response) => {
      if (!active || activeRequestKeyRef.current !== requestKey) {
        return;
      }
      setPageState({
        requestKey,
        items: response.items,
        nextCursor: response.nextCursor,
        error: null,
        loadMoreError: null,
      });
    }).catch((requestError: unknown) => {
      if (!active || activeRequestKeyRef.current !== requestKey) {
        return;
      }
      setPageState({
        requestKey,
        items: [],
        nextCursor: null,
        error: getParsedApiError(requestError),
        loadMoreError: null,
      });
    });

    return () => {
      active = false;
    };
  }, [normalizedTaskId, requestKey, shouldLoad]);

  const hasFreshPage = shouldLoad && pageState.requestKey === requestKey;
  const items = hasFreshPage ? pageState.items : [];
  const nextCursor = hasFreshPage ? pageState.nextCursor : null;

  const loadMore = useCallback(async () => {
    if (!shouldLoad || !hasFreshPage || !nextCursor || isLoadingMore) {
      return;
    }
    const loadMoreRequest = loadMoreRequestRef.current + 1;
    loadMoreRequestRef.current = loadMoreRequest;
    const expectedRequestKey = requestKey;
    setIsLoadingMore(true);
    setPageState((current) => (
      current.requestKey === expectedRequestKey
        ? { ...current, loadMoreError: null }
        : current
    ));
    try {
      const response = await researchApi.listDebates({
        jobId: normalizedTaskId,
        cursor: nextCursor,
        limit: DEBATE_PAGE_SIZE,
      });
      if (
        activeRequestKeyRef.current !== expectedRequestKey
        || loadMoreRequestRef.current !== loadMoreRequest
      ) {
        return;
      }
      setPageState((current) => (
        current.requestKey === expectedRequestKey
          ? {
              ...current,
              items: mergeDebates(current.items, response.items),
              nextCursor: response.nextCursor,
              loadMoreError: null,
            }
          : current
      ));
    } catch (requestError: unknown) {
      if (
        activeRequestKeyRef.current === expectedRequestKey
        && loadMoreRequestRef.current === loadMoreRequest
      ) {
        setPageState((current) => (
          current.requestKey === expectedRequestKey
            ? { ...current, loadMoreError: getParsedApiError(requestError) }
            : current
        ));
      }
    } finally {
      if (
        activeRequestKeyRef.current === expectedRequestKey
        && loadMoreRequestRef.current === loadMoreRequest
      ) {
        setIsLoadingMore(false);
      }
    }
  }, [
    hasFreshPage,
    isLoadingMore,
    nextCursor,
    normalizedTaskId,
    requestKey,
    shouldLoad,
  ]);

  const loadDetail = useCallback(async (debateHash: string, force = false) => {
    const normalizedHash = debateHash.trim();
    if (!shouldLoad || !normalizedHash) {
      return;
    }
    const existing = details[normalizedHash] || EMPTY_DETAIL;
    if (!force && (existing.detail || existing.isLoading)) {
      return;
    }
    const detailRequest = (detailRequestRef.current[normalizedHash] || 0) + 1;
    detailRequestRef.current[normalizedHash] = detailRequest;
    const expectedRequestKey = requestKey;
    setDetails((current) => ({
      ...current,
      [normalizedHash]: {
        detail: force ? null : current[normalizedHash]?.detail || null,
        isLoading: true,
        error: null,
      },
    }));
    try {
      const detail = await researchApi.getDebate(normalizedHash);
      if (
        activeRequestKeyRef.current !== expectedRequestKey
        || detailRequestRef.current[normalizedHash] !== detailRequest
      ) {
        return;
      }
      setDetails((current) => ({
        ...current,
        [normalizedHash]: { detail, isLoading: false, error: null },
      }));
    } catch (requestError: unknown) {
      if (
        activeRequestKeyRef.current === expectedRequestKey
        && detailRequestRef.current[normalizedHash] === detailRequest
      ) {
        setDetails((current) => ({
          ...current,
          [normalizedHash]: {
            detail: null,
            isLoading: false,
            error: getParsedApiError(requestError),
          },
        }));
      }
    }
  }, [details, requestKey, shouldLoad]);

  return {
    items,
    nextCursor,
    isLoading: shouldLoad && !hasFreshPage,
    isLoadingMore,
    error: hasFreshPage ? pageState.error : null,
    loadMoreError: hasFreshPage ? pageState.loadMoreError : null,
    details,
    refetch,
    loadMore,
    loadDetail,
  };
}
