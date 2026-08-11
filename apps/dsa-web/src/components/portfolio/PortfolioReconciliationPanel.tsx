import type React from 'react';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { portfolioApi } from '../../api/portfolio';
import { getParsedApiError } from '../../api/error';
import { Card, InlineAlert } from '../common';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import type {
  PortfolioAccountItem,
  PortfolioReconciliationCashTarget,
  PortfolioReconciliationEventType,
  PortfolioReconciliationItem,
  PortfolioReconciliationPositionTarget,
  PortfolioReconciliationPreviewResponse,
} from '../../types/portfolio';
import { getTodayIso } from '../../utils/portfolioFormat';

type Props = {
  accounts: PortfolioAccountItem[];
  preferredAccountId?: number;
  onApplied?: () => void | Promise<void>;
};

const INPUT_CLASS = 'input-surface input-focus-glow h-10 w-full rounded-xl border bg-transparent px-3 text-sm focus:outline-none';
const TEXTAREA_CLASS = 'input-surface input-focus-glow min-h-28 w-full rounded-xl border bg-transparent px-3 py-2 font-mono text-xs focus:outline-none';

function createIdempotencyKey(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `reconciliation-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function parseArray<T>(raw: string, label: string): T[] {
  const parsed: unknown = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON array`);
  }
  return parsed as T[];
}

async function fetchReconciliationHistory(
  accountId: number,
): Promise<PortfolioReconciliationItem[]> {
  if (!accountId) return [];
  const response = await portfolioApi.listReconciliations(accountId, false);
  return response.items;
}

export const PortfolioReconciliationPanel: React.FC<Props> = ({
  accounts,
  preferredAccountId,
  onApplied,
}) => {
  const { language } = useUiLanguage();
  const copy = language === 'en' ? {
    title: 'Opening & reconciliation',
    description: 'Preview an authoritative broker state, review the diff, then append an immutable non-trade adjustment.',
    account: 'Account',
    type: 'Event type',
    opening: 'Opening',
    adjustment: 'Reconciliation',
    date: 'Effective date',
    source: 'Source',
    note: 'Note',
    cash: 'Absolute cash targets (JSON)',
    positions: 'Absolute position targets (JSON)',
    preview: 'Preview',
    apply: 'Apply preview',
    loading: 'Working…',
    noAccount: 'Create and select an account first.',
    history: 'Applied history',
    empty: 'No applied opening/reconciliation events.',
    applied: 'Reconciliation applied. Portfolio replay now uses this immutable state-set event.',
    warning: 'Applying a position reconciliation resets FIFO lots to one audited baseline lot at the effective date.',
  } : {
    title: '期初与对账',
    description: '先预览券商绝对状态差异，再追加不可变的非交易调整事件。',
    account: '账户',
    type: '事件类型',
    opening: '期初',
    adjustment: '对账',
    date: '生效日期',
    source: '数据来源',
    note: '备注',
    cash: '绝对现金目标（JSON）',
    positions: '绝对持仓目标（JSON）',
    preview: '预览差异',
    apply: '应用预览',
    loading: '处理中…',
    noAccount: '请先创建并选择账户。',
    history: '已应用历史',
    empty: '尚无已应用的期初/对账事件。',
    applied: '对账已应用，Portfolio replay 将使用该不可变 state-set 事件。',
    warning: '持仓对账会在生效时点把 FIFO lots 重置为一个带审计 before/after 的基准 lot。',
  };

  const initialAccount = useMemo(() => {
    if (preferredAccountId && accounts.some((item) => item.id === preferredAccountId)) {
      return preferredAccountId;
    }
    return accounts[0]?.id ?? 0;
  }, [accounts, preferredAccountId]);
  const [selectedAccountId, setSelectedAccountId] = useState<number | null>(null);
  const accountId = selectedAccountId !== null
    && accounts.some((item) => item.id === selectedAccountId)
    ? selectedAccountId
    : initialAccount;
  const [eventType, setEventType] = useState<PortfolioReconciliationEventType>('adjustment');
  const [effectiveDate, setEffectiveDate] = useState(getTodayIso());
  const [source, setSource] = useState('broker_statement');
  const [note, setNote] = useState('');
  const [cashJson, setCashJson] = useState('[{"currency":"CNY","balance":0}]');
  const [positionsJson, setPositionsJson] = useState('[]');
  const [preview, setPreview] = useState<PortfolioReconciliationPreviewResponse | null>(null);
  const [idempotencyKey, setIdempotencyKey] = useState<string | null>(null);
  const [history, setHistory] = useState<PortfolioReconciliationItem[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  const invalidatePreview = () => {
    setPreview(null);
    setIdempotencyKey(null);
  };

  const refreshHistory = useCallback(async () => {
    try {
      setHistory(await fetchReconciliationHistory(accountId));
    } catch (err) {
      setError(getParsedApiError(err).message);
    }
  }, [accountId]);

  useEffect(() => {
    let cancelled = false;
    void fetchReconciliationHistory(accountId)
      .then((items) => {
        if (!cancelled) setHistory(items);
      })
      .catch((err: unknown) => {
        if (!cancelled) setError(getParsedApiError(err).message);
      });
    return () => {
      cancelled = true;
    };
  }, [accountId]);

  const handlePreview = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!accountId) return;
    setBusy(true);
    setError(null);
    setSuccess(null);
    invalidatePreview();
    try {
      const cash = parseArray<PortfolioReconciliationCashTarget>(cashJson, copy.cash);
      const positions = parseArray<PortfolioReconciliationPositionTarget>(positionsJson, copy.positions);
      const response = await portfolioApi.previewReconciliation(accountId, {
        eventType,
        effectiveDate,
        cash,
        positions,
        source,
        note: note || null,
      });
      setPreview(response);
      setIdempotencyKey(createIdempotencyKey());
    } catch (err) {
      setError(err instanceof Error ? err.message : getParsedApiError(err).message);
    } finally {
      setBusy(false);
    }
  };

  const handleApply = async () => {
    if (!accountId || !preview || !idempotencyKey) return;
    setBusy(true);
    setError(null);
    setSuccess(null);
    try {
      await portfolioApi.applyReconciliation(accountId, {
        previewToken: preview.previewToken,
        idempotencyKey,
      });
      invalidatePreview();
      setSuccess(copy.applied);
      await refreshHistory();
      await onApplied?.();
    } catch (err) {
      setError(getParsedApiError(err).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card padding="md">
      <div className="space-y-1">
        <h2 className="text-sm font-semibold text-foreground">{copy.title}</h2>
        <p className="text-xs text-secondary">{copy.description}</p>
      </div>
      {!accounts.length ? <InlineAlert variant="warning" className="mt-3" message={copy.noAccount} /> : null}
      {error ? <InlineAlert variant="danger" className="mt-3" message={error} /> : null}
      {success ? <InlineAlert variant="success" className="mt-3" message={success} /> : null}
      <InlineAlert variant="warning" className="mt-3" message={copy.warning} />

      <form className="mt-3 space-y-3" onSubmit={handlePreview}>
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-2">
          <label className="text-xs text-secondary">
            {copy.account}
            <select className={`${INPUT_CLASS} mt-1`} value={accountId || ''} onChange={(event) => {
              setSelectedAccountId(Number(event.target.value));
              invalidatePreview();
            }} disabled={!accounts.length || busy}>
              {accounts.map((account) => <option key={account.id} value={account.id}>{account.name} (#{account.id})</option>)}
            </select>
          </label>
          <label className="text-xs text-secondary">
            {copy.type}
            <select className={`${INPUT_CLASS} mt-1`} value={eventType} onChange={(event) => {
              setEventType(event.target.value as PortfolioReconciliationEventType);
              invalidatePreview();
            }} disabled={busy}>
              <option value="opening">{copy.opening}</option>
              <option value="adjustment">{copy.adjustment}</option>
            </select>
          </label>
          <label className="text-xs text-secondary">
            {copy.date}
            <input className={`${INPUT_CLASS} mt-1`} type="date" value={effectiveDate} onChange={(event) => {
              setEffectiveDate(event.target.value);
              invalidatePreview();
            }} required disabled={busy} />
          </label>
          <label className="text-xs text-secondary">
            {copy.source}
            <input className={`${INPUT_CLASS} mt-1`} value={source} onChange={(event) => {
              setSource(event.target.value);
              invalidatePreview();
            }} required maxLength={64} disabled={busy} />
          </label>
        </div>
        <label className="block text-xs text-secondary">
          {copy.note}
          <input className={`${INPUT_CLASS} mt-1`} value={note} onChange={(event) => {
            setNote(event.target.value);
            invalidatePreview();
          }} maxLength={255} disabled={busy} />
        </label>
        <div className="grid grid-cols-1 xl:grid-cols-2 gap-3">
          <label className="text-xs text-secondary">
            {copy.cash}
            <textarea aria-label={copy.cash} className={`${TEXTAREA_CLASS} mt-1`} value={cashJson} onChange={(event) => {
              setCashJson(event.target.value);
              invalidatePreview();
            }} disabled={busy} />
          </label>
          <label className="text-xs text-secondary">
            {copy.positions}
            <textarea aria-label={copy.positions} className={`${TEXTAREA_CLASS} mt-1`} value={positionsJson} onChange={(event) => {
              setPositionsJson(event.target.value);
              invalidatePreview();
            }} disabled={busy} />
          </label>
        </div>
        <div className="flex flex-wrap gap-2">
          <button className="btn-secondary text-sm" type="submit" disabled={!accountId || busy}>
            {busy ? copy.loading : copy.preview}
          </button>
          {preview ? (
            <button className="btn-primary text-sm" type="button" onClick={() => void handleApply()} disabled={busy}>
              {busy ? copy.loading : copy.apply}
            </button>
          ) : null}
        </div>
      </form>

      {preview ? (
        <div className="mt-3 rounded-xl border border-white/10 bg-white/[0.02] p-3 text-xs text-secondary">
          <div>#{preview.id} · {preview.eventType} · {preview.effectiveDate}</div>
          <div>{preview.diff.adjustments?.length ?? 0} adjustment(s)</div>
          {preview.warnings.map((warning) => <div key={warning} className="text-warning">{warning}</div>)}
        </div>
      ) : null}

      <div className="mt-4">
        <h3 className="text-xs font-semibold text-foreground">{copy.history}</h3>
        {history.length ? (
          <div className="mt-2 space-y-1 text-xs text-secondary">
            {history.map((item) => (
              <div key={item.id} className="flex flex-wrap justify-between gap-2 rounded-lg border border-white/10 px-3 py-2">
                <span>v{item.eventVersion} · {item.eventType} · {item.effectiveDate}</span>
                <span>{item.status}</span>
              </div>
            ))}
          </div>
        ) : <p className="mt-2 text-xs text-secondary">{copy.empty}</p>}
      </div>
    </Card>
  );
};
