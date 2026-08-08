import { useCallback, useEffect, useRef, useState } from 'react';
import { researchApi } from '../api/research';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import type {
  ResearchEvidenceDetailResponse,
  ResearchEvidenceSummary,
} from '../types/research';

const EVIDENCE_PAGE_SIZE = 20;

type EvidencePageState = {
  requestKey: string;
  items: ResearchEvidenceSummary[];
  nextCursor: string | null;
  error: ParsedApiError | null;
  loadMoreError: ParsedApiError | null;
};

export type ResearchEvidenceDetailState = {
  detail: ResearchEvidenceDetailResponse | null;
  isLoading: boolean;
  error: ParsedApiError | null;
};

interface UseResearchEvidenceOptions {
  taskId?: string | null;
  enabled?: boolean;
}

interface UseResearchEvidenceResult {
  items: ResearchEvidenceSummary[];
  nextCursor: string | null;
  isLoading: boolean;
  isLoadingMore: boolean;
  error: ParsedApiError | null;
  loadMoreError: ParsedApiError | null;
  details: Record<string, ResearchEvidenceDetailState>;
  refetch: () => void;
  loadMore: () => Promise<void>;
  loadDetail: (evidenceHash: string, force?: boolean) => Promise<void>;
}

const EMPTY_DETAIL: ResearchEvidenceDetailState = {
  detail: null,
  isLoading: false,
  error: null,
};

const mergeEvidence = (
  current: ResearchEvidenceSummary[],
  incoming: ResearchEvidenceSummary[],
): ResearchEvidenceSummary[] => {
  const byHash = new Map(current.map((item) => [item.evidenceHash, item]));
  incoming.forEach((item) => byHash.set(item.evidenceHash, item));
  return Array.from(byHash.values());
};

export function useResearchEvidence({
  taskId,
  enabled = true,
}: UseResearchEvidenceOptions): UseResearchEvidenceResult {
  const normalizedTaskId = taskId?.trim() || '';
  const sourceKey = normalizedTaskId ? `task:${normalizedTaskId}` : 'none';
  const [reloadToken, setReloadToken] = useState(0);
  const requestKey = `${sourceKey}:${reloadToken}`;
  const shouldLoad = enabled && Boolean(normalizedTaskId);
  const [pageState, setPageState] = useState<EvidencePageState>({
    requestKey: 'none',
    items: [],
    nextCursor: null,
    error: null,
    loadMoreError: null,
  });
  const [isLoadingMore, setIsLoadingMore] = useState(false);
  const [details, setDetails] = useState<Record<string, ResearchEvidenceDetailState>>({});
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
    researchApi.listEvidence({
      jobId: normalizedTaskId,
      limit: EVIDENCE_PAGE_SIZE,
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
      const response = await researchApi.listEvidence({
        jobId: normalizedTaskId,
        cursor: nextCursor,
        limit: EVIDENCE_PAGE_SIZE,
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
              items: mergeEvidence(current.items, response.items),
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

  const loadDetail = useCallback(async (evidenceHash: string, force = false) => {
    const normalizedHash = evidenceHash.trim();
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
      const detail = await researchApi.getEvidence(normalizedHash);
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
