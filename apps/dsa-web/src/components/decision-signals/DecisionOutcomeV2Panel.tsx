import type React from 'react';
import { BarChart3, RefreshCw } from 'lucide-react';
import { ApiErrorAlert, EmptyState } from '../common';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import { useDecisionOutcomesV2 } from '../../hooks/useDecisionOutcomesV2';
import type {
  DecisionOutcomeV2BenchmarkItem,
  DecisionOutcomeV2Item,
  DecisionOutcomeV2StatsBucket,
} from '../../types/decisionOutcomesV2';

function formatNumber(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return value.toFixed(digits).replace(/0+$/u, '').replace(/\.$/u, '');
}

function formatPercent(value: number | null | undefined): string {
  const formatted = formatNumber(value);
  return formatted === '—' ? formatted : `${formatted}%`;
}

function formatRatioPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return `${formatNumber(value * 100)}%`;
}

function metric(label: string, value: string): React.ReactNode {
  return (
    <div className="rounded-lg border border-border/50 bg-elevated/30 px-3 py-2">
      <p className="text-[11px] text-secondary-text">{label}</p>
      <p className="mt-1 text-sm font-semibold text-foreground">{value}</p>
    </div>
  );
}

function benchmarkText(
  benchmark: DecisionOutcomeV2BenchmarkItem,
  fallbackName: string,
): string {
  const label = benchmark.name || benchmark.code || fallbackName;
  if (benchmark.status !== 'available') {
    return `${label}: ${benchmark.reasonCode || '—'}`;
  }
  return `${label}: ${formatPercent(benchmark.directionalExcessReturnPct)}`;
}

const OutcomeRow: React.FC<{ item: DecisionOutcomeV2Item }> = ({ item }) => {
  const { t } = useUiLanguage();
  return (
    <article className="rounded-xl border border-border/60 bg-elevated/25 p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <p className="text-sm font-semibold text-foreground">
            {item.stockCode} · {item.horizon}
          </p>
          <p className="mt-1 text-xs text-secondary-text">
            {t('decisionSignals.outcomeV2.signalId', { id: item.signalId })}
            {' · '}{item.decisionProfile} · {item.finalActionFamily}
          </p>
        </div>
        <span className="rounded-full border border-border/60 px-2 py-1 text-xs text-secondary-text">
          {item.evalStatus}
        </span>
      </div>
      <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        {metric(
          t('decisionSignals.outcomeV2.t1Execution'),
          item.entryTradeDate
            ? `${item.entryTradeDate} · ${item.executionStatus}`
            : item.executionStatus,
        )}
        {metric(t('decisionSignals.outcomeV2.directionalReturn'), formatPercent(item.directionalReturnPct))}
        {metric(t('decisionSignals.outcomeV2.mfe'), formatPercent(item.mfePct))}
        {metric(t('decisionSignals.outcomeV2.mae'), formatPercent(item.maePct))}
      </div>
      <div className="mt-3 grid gap-2 text-xs text-secondary-text md:grid-cols-2">
        <p>{benchmarkText(item.csi300, 'CSI 300')}</p>
        <p>{benchmarkText(item.sw1, 'SW1')}</p>
      </div>
      {item.reasonCode ? (
        <p className="mt-3 break-words text-xs text-warning">
          {t('decisionSignals.outcomeV2.reason')}: {item.reasonCode}
        </p>
      ) : null}
    </article>
  );
};

const CalibrationBucket: React.FC<{
  bucket: DecisionOutcomeV2StatsBucket;
  minimum: number;
}> = ({ bucket, minimum }) => {
  const { t } = useUiLanguage();
  const { dimensions } = bucket;
  return (
    <div className="rounded-xl border border-border/60 bg-elevated/25 p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <p className="text-sm font-semibold text-foreground">
          {dimensions.horizon} · {dimensions.profile} · {dimensions.finalActionFamily}
        </p>
        <span className={bucket.sampleSufficient ? 'text-xs text-success' : 'text-xs text-warning'}>
          n={bucket.calibrationSamples}/{minimum}
        </span>
      </div>
      {!bucket.sampleSufficient ? (
        <p className="mt-2 text-xs text-warning">
          {t('decisionSignals.outcomeV2.insufficient')}
        </p>
      ) : null}
      <div className="mt-3 grid grid-cols-3 gap-2">
        {metric(t('decisionSignals.outcomeV2.accuracy'), formatRatioPercent(bucket.accuracy))}
        {metric('ECE', formatNumber(bucket.ece, 4))}
        {metric(t('decisionSignals.outcomeV2.brier'), formatNumber(bucket.brierScore, 4))}
      </div>
      <div className="mt-3 grid gap-2 text-xs text-secondary-text md:grid-cols-2">
        <p>
          CSI 300 · n={bucket.benchmarks.csi300.samples}/{minimum} · {' '}
          {formatPercent(bucket.benchmarks.csi300.meanDirectionalExcessReturnPct)}
        </p>
        <p>
          SW1 · n={bucket.benchmarks.sw1.samples}/{minimum} · {' '}
          {formatPercent(bucket.benchmarks.sw1.meanDirectionalExcessReturnPct)}
        </p>
      </div>
    </div>
  );
};

export const DecisionOutcomeV2Panel: React.FC = () => {
  const { t } = useUiLanguage();
  const { items, total, stats, isLoading, error, refetch } = useDecisionOutcomesV2();

  if (error) {
    return (
      <ApiErrorAlert
        error={{ ...error, title: t('decisionSignals.outcomeV2.errorTitle') }}
        actionLabel={t('common.retry')}
        onAction={refetch}
      />
    );
  }

  if (isLoading) {
    return <p className="text-sm text-secondary-text">{t('common.loading')}...</p>;
  }

  if (!stats || (total === 0 && stats.buckets.length === 0)) {
    return (
      <EmptyState
        className="border-none bg-transparent py-6 shadow-none"
        title={t('decisionSignals.outcomeV2.emptyTitle')}
        description={t('decisionSignals.outcomeV2.emptyDescription')}
        icon={<BarChart3 className="h-6 w-6" />}
      />
    );
  }

  return (
    <div className="space-y-4" data-testid="decision-outcome-v2-panel">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-secondary-text">
          {t('decisionSignals.outcomeV2.summary', {
            total,
            buckets: stats.buckets.length,
            minimum: stats.minimumCompletedSampleSize,
          })}
        </p>
        <button type="button" className="btn-secondary inline-flex items-center gap-2" onClick={refetch}>
          <RefreshCw className="h-4 w-4" />
          {t('decisionSignals.outcomeV2.refresh')}
        </button>
      </div>

      {stats.buckets.length > 0 ? (
        <section aria-label={t('decisionSignals.outcomeV2.calibrationTitle')}>
          <h4 className="text-sm font-semibold text-foreground">
            {t('decisionSignals.outcomeV2.calibrationTitle')}
          </h4>
          <div className="mt-2 grid gap-3 xl:grid-cols-2">
            {stats.buckets.slice(0, 6).map((bucket) => (
              <CalibrationBucket
                key={[
                  bucket.dimensions.engine,
                  bucket.dimensions.horizon,
                  bucket.dimensions.profile,
                  bucket.dimensions.finalActionFamily,
                ].join(':')}
                bucket={bucket}
                minimum={stats.minimumCompletedSampleSize}
              />
            ))}
          </div>
        </section>
      ) : null}

      {items.length > 0 ? (
        <section aria-label={t('decisionSignals.outcomeV2.recentTitle')}>
          <h4 className="text-sm font-semibold text-foreground">
            {t('decisionSignals.outcomeV2.recentTitle')}
          </h4>
          <div className="mt-2 grid gap-3 xl:grid-cols-2">
            {items.slice(0, 10).map((item) => <OutcomeRow key={item.id} item={item} />)}
          </div>
        </section>
      ) : null}
    </div>
  );
};
