import { useCallback, useEffect, useState } from 'react';
import { decisionOutcomesV2Api } from '../api/decisionOutcomesV2';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import type {
  DecisionOutcomeV2Item,
  DecisionOutcomeV2StatsResponse,
} from '../types/decisionOutcomesV2';

interface UseDecisionOutcomesV2Options {
  enabled?: boolean;
  pageSize?: number;
}

interface UseDecisionOutcomesV2Result {
  items: DecisionOutcomeV2Item[];
  total: number;
  stats: DecisionOutcomeV2StatsResponse | null;
  isLoading: boolean;
  error: ParsedApiError | null;
  refetch: () => void;
}

export function useDecisionOutcomesV2({
  enabled = true,
  pageSize = 20,
}: UseDecisionOutcomesV2Options = {}): UseDecisionOutcomesV2Result {
  const [reloadToken, setReloadToken] = useState(0);
  const requestKey = `${pageSize}:${reloadToken}`;
  const [state, setState] = useState<{
    requestKey: string;
    items: DecisionOutcomeV2Item[];
    total: number;
    stats: DecisionOutcomeV2StatsResponse | null;
    error: ParsedApiError | null;
  }>({
    requestKey: 'none',
    items: [],
    total: 0,
    stats: null,
    error: null,
  });

  const refetch = useCallback(() => {
    setReloadToken((value) => value + 1);
  }, []);

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }
    let active = true;
    Promise.all([
      decisionOutcomesV2Api.list({ page: 1, pageSize }),
      decisionOutcomesV2Api.getStats(),
    ]).then(([listResponse, statsResponse]) => {
      if (!active) return;
      setState({
        requestKey,
        items: listResponse.items,
        total: listResponse.total,
        stats: statsResponse,
        error: null,
      });
    }).catch((requestError: unknown) => {
      if (!active) return;
      setState({
        requestKey,
        items: [],
        total: 0,
        stats: null,
        error: getParsedApiError(requestError),
      });
    });
    return () => {
      active = false;
    };
  }, [enabled, pageSize, requestKey]);

  const fresh = enabled && state.requestKey === requestKey;

  return {
    items: fresh ? state.items : [],
    total: fresh ? state.total : 0,
    stats: fresh ? state.stats : null,
    isLoading: enabled && !fresh,
    error: fresh ? state.error : null,
    refetch,
  };
}
