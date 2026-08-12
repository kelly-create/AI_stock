import type React from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  AlertTriangle,
  ArrowRight,
  BrainCircuit,
  CheckCircle2,
  Clock3,
  Sparkles,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { analysisApi } from '../../api/analysis';
import { getParsedApiError, type ParsedApiError } from '../../api/error';
import { personalResearchApi } from '../../api/personalResearch';
import { researchApi } from '../../api/research';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import type { UiTextKey } from '../../i18n/uiText';
import type { TaskStatus } from '../../types/analysis';
import type {
  PersonalResearchMode,
  PersonalResearchRunAccepted,
} from '../../types/personalResearch';
import { normalizeStockCode } from '../../utils/stockCode';
import { ApiErrorAlert, Button, Drawer, InlineAlert } from '../common';

const TRACKED_RUN_STORAGE_KEY = 'dsa.personalResearch.trackedRun';
const POLL_INTERVAL_MS = 1_500;

type TrackedRun = Pick<
  PersonalResearchRunAccepted,
  'taskId' | 'stockCode' | 'requestedMode' | 'resolvedMode'
>;

type PersonalResearchLauncherProps = {
  stockCode: string;
  notify: boolean;
  reportLanguage?: string;
  disabled?: boolean;
  onTaskAccepted?: () => void | Promise<void>;
};

const MODE_OPTIONS: Array<{
  mode: PersonalResearchMode;
  labelKey: UiTextKey;
  descriptionKey: UiTextKey;
}> = [
  { mode: 'auto', labelKey: 'personalResearch.modeAuto', descriptionKey: 'personalResearch.modeAutoDescription' },
  { mode: 'quick', labelKey: 'personalResearch.modeQuick', descriptionKey: 'personalResearch.modeQuickDescription' },
  { mode: 'standard', labelKey: 'personalResearch.modeStandard', descriptionKey: 'personalResearch.modeStandardDescription' },
  { mode: 'deep', labelKey: 'personalResearch.modeDeep', descriptionKey: 'personalResearch.modeDeepDescription' },
  { mode: 'debate', labelKey: 'personalResearch.modeDebate', descriptionKey: 'personalResearch.modeDebateDescription' },
];

function loadTrackedRun(): TrackedRun | null {
  try {
    const raw = window.sessionStorage.getItem(TRACKED_RUN_STORAGE_KEY);
    if (!raw) return null;
    const value = JSON.parse(raw) as Partial<TrackedRun>;
    if (
      typeof value.taskId !== 'string'
      || typeof value.stockCode !== 'string'
      || !MODE_OPTIONS.some((item) => item.mode === value.requestedMode)
      || !['quick', 'standard', 'deep', 'debate'].includes(String(value.resolvedMode))
    ) {
      return null;
    }
    return value as TrackedRun;
  } catch {
    return null;
  }
}

function createIdempotencyKey(): string {
  const randomPart = typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
  return `web-personal-${randomPart}`;
}

function isTerminal(status: TaskStatus['status'] | undefined): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled';
}

export const PersonalResearchLauncher: React.FC<PersonalResearchLauncherProps> = ({
  stockCode,
  notify,
  reportLanguage,
  disabled = false,
  onTaskAccepted,
}) => {
  const { t } = useUiLanguage();
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<PersonalResearchMode>('auto');
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<ParsedApiError | null>(null);
  const [trackedRun, setTrackedRun] = useState<TrackedRun | null>(loadTrackedRun);
  const [taskStatus, setTaskStatus] = useState<TaskStatus | null>(null);
  const [decisionSignalId, setDecisionSignalId] = useState<number | null>(null);
  const [resultLookupFailed, setResultLookupFailed] = useState(false);
  const idempotencyKeyRef = useRef<string | null>(null);
  const idempotencyScopeRef = useRef<string | null>(null);

  const normalizedStockCode = useMemo(() => normalizeStockCode(stockCode), [stockCode]);
  const validStockCode = /^\d{6}$/.test(normalizedStockCode);
  const progress = taskStatus?.progress ?? (trackedRun ? 0 : 0);
  const running = Boolean(trackedRun && !isTerminal(taskStatus?.status));

  useEffect(() => {
    if (!trackedRun || isTerminal(taskStatus?.status)) return undefined;
    let cancelled = false;
    let timer: number | undefined;

    const poll = async () => {
      try {
        const nextStatus = await analysisApi.getStatus(trackedRun.taskId);
        if (cancelled) return;
        setTaskStatus(nextStatus);
        if (nextStatus.status === 'completed') return;
        if (nextStatus.status === 'failed' || nextStatus.status === 'cancelled') return;
      } catch {
        if (cancelled) return;
      }
      if (!cancelled) timer = window.setTimeout(() => void poll(), POLL_INTERVAL_MS);
    };

    queueMicrotask(() => void poll());
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [taskStatus?.status, trackedRun]);

  useEffect(() => {
    if (taskStatus?.status !== 'completed' || !trackedRun) return undefined;
    let cancelled = false;
    const summaryId = taskStatus.result?.personalResearch?.decisionSignal?.id;
    if (typeof summaryId === 'number' && Number.isInteger(summaryId) && summaryId > 0) {
      return () => {
        cancelled = true;
      };
    }

    void researchApi.getLatestPersonalResearchThesis({
      taskId: trackedRun.taskId,
      market: 'cn',
      stockCode: trackedRun.stockCode,
    }).then((thesis) => {
      if (cancelled) return;
      setDecisionSignalId(thesis.lineage.decisionSignalId);
      setResultLookupFailed(thesis.lineage.decisionSignalId === null);
    }).catch(() => {
      if (!cancelled) setResultLookupFailed(true);
    });
    return () => {
      cancelled = true;
    };
  }, [taskStatus, trackedRun]);

  const handleModeChange = (nextMode: PersonalResearchMode) => {
    setMode(nextMode);
    setSubmitError(null);
    idempotencyKeyRef.current = null;
  };

  const handleSubmit = async () => {
    if (!validStockCode || submitting || running) return;
    setSubmitting(true);
    setSubmitError(null);
    setResultLookupFailed(false);
    setDecisionSignalId(null);
    const idempotencyScope = `${normalizedStockCode}:${mode}`;
    if (idempotencyScopeRef.current !== idempotencyScope) {
      idempotencyKeyRef.current = null;
      idempotencyScopeRef.current = idempotencyScope;
    }
    const idempotencyKey = idempotencyKeyRef.current ?? createIdempotencyKey();
    idempotencyKeyRef.current = idempotencyKey;
    try {
      const accepted = await personalResearchApi.createRun(
        {
          stockCode: normalizedStockCode,
          requestedMode: mode,
          notify,
          reportLanguage,
        },
        idempotencyKey,
      );
      const nextTrackedRun: TrackedRun = {
        taskId: accepted.taskId,
        stockCode: accepted.stockCode,
        requestedMode: accepted.requestedMode,
        resolvedMode: accepted.resolvedMode,
      };
      window.sessionStorage.setItem(TRACKED_RUN_STORAGE_KEY, JSON.stringify(nextTrackedRun));
      setTrackedRun(nextTrackedRun);
      setTaskStatus({
        taskId: accepted.taskId,
        traceId: accepted.traceId,
        status: accepted.status,
        stage: 'personal_research',
        progress: 0,
      });
      idempotencyKeyRef.current = null;
      idempotencyScopeRef.current = null;
      await onTaskAccepted?.();
    } catch (error) {
      setSubmitError(getParsedApiError(error));
    } finally {
      setSubmitting(false);
    }
  };

  const handleDismissResult = () => {
    window.sessionStorage.removeItem(TRACKED_RUN_STORAGE_KEY);
    setTrackedRun(null);
    setTaskStatus(null);
    setDecisionSignalId(null);
    setResultLookupFailed(false);
  };

  const handleOpenResult = () => {
    const summaryId = taskStatus?.result?.personalResearch?.decisionSignal?.id;
    const targetSignalId = decisionSignalId
      ?? (typeof summaryId === 'number' && Number.isInteger(summaryId) && summaryId > 0 ? summaryId : null);
    if (targetSignalId !== null) {
      navigate(`/decision-signals?signalId=${targetSignalId}`);
      setOpen(false);
      return;
    }
    const sourceReportId = taskStatus?.result?.report?.meta?.id;
    if (sourceReportId) {
      navigate(`/decision-signals?sourceReportId=${sourceReportId}`);
      setOpen(false);
    }
  };

  const taskFailed = taskStatus?.status === 'failed' || taskStatus?.status === 'cancelled';
  const taskCompleted = taskStatus?.status === 'completed';
  const summarySignalId = taskStatus?.result?.personalResearch?.decisionSignal?.id;
  const hasResultTarget = decisionSignalId !== null
    || (typeof summarySignalId === 'number' && Number.isInteger(summarySignalId) && summarySignalId > 0)
    || Boolean(taskStatus?.result?.report?.meta?.id);
  const resolvedModeLabel = trackedRun
    ? MODE_OPTIONS.find((option) => option.mode === trackedRun.resolvedMode)?.labelKey
    : undefined;

  return (
    <>
      <Button
        type="button"
        variant="gradient"
        size="md"
        disabled={disabled || (!validStockCode && !trackedRun)}
        onClick={() => setOpen(true)}
        className="h-10 flex-1 whitespace-nowrap md:flex-none"
      >
        {running ? <Clock3 className="h-4 w-4 animate-pulse" aria-hidden="true" /> : <BrainCircuit className="h-4 w-4" aria-hidden="true" />}
        {taskCompleted ? t('personalResearch.viewResult') : t('personalResearch.launch')}
      </Button>

      {createPortal(<Drawer
        isOpen={open}
        onClose={() => setOpen(false)}
        title={t('personalResearch.title')}
        width="max-w-xl"
        zIndex={140}
      >
        <div className="space-y-5">
          <div className="rounded-2xl border border-cyan/20 bg-cyan/5 p-4">
            <div className="flex items-center gap-2 text-sm font-semibold text-foreground">
              <Sparkles className="h-4 w-4 text-cyan" aria-hidden="true" />
              {t('personalResearch.formalWorkflow')}
            </div>
            <p className="mt-2 text-sm leading-6 text-secondary-text">
              {t('personalResearch.description')}
            </p>
          </div>

          {trackedRun ? (
            <section aria-live="polite" className="space-y-4 rounded-2xl border border-border/70 bg-surface/60 p-4">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <p className="text-sm font-semibold text-foreground">
                    {taskCompleted ? t('personalResearch.completed') : taskFailed ? t('personalResearch.failed') : t('personalResearch.running')}
                  </p>
                  <p className="mt-1 text-xs text-secondary-text">
                    {trackedRun.stockCode} · {resolvedModeLabel ? t(resolvedModeLabel) : trackedRun.resolvedMode}
                  </p>
                </div>
                {taskCompleted ? <CheckCircle2 className="h-5 w-5 text-success" aria-hidden="true" /> : taskFailed ? <AlertTriangle className="h-5 w-5 text-danger" aria-hidden="true" /> : <Clock3 className="h-5 w-5 animate-pulse text-cyan" aria-hidden="true" />}
              </div>
              {!taskFailed ? (
                <div>
                  <div className="mb-1 flex justify-between text-xs text-secondary-text">
                    <span>{taskStatus?.stage || t('personalResearch.queued')}</span>
                    <span>{Math.max(0, Math.min(100, progress))}%</span>
                  </div>
                  <div className="h-2 overflow-hidden rounded-full bg-hover">
                    <div
                      className="h-full rounded-full bg-primary-gradient transition-[width] duration-500"
                      style={{ width: `${Math.max(2, Math.min(100, progress))}%` }}
                    />
                  </div>
                </div>
              ) : null}
              {taskFailed ? (
                <InlineAlert
                  variant="danger"
                  title={t('personalResearch.failed')}
                  message={taskStatus?.error || t('personalResearch.failedDescription')}
                />
              ) : null}
              {taskCompleted && resultLookupFailed ? (
                <InlineAlert
                  variant="warning"
                  title={t('personalResearch.resultPending')}
                  message={t('personalResearch.resultPendingDescription')}
                />
              ) : null}
              <div className="flex flex-wrap justify-end gap-2">
                {isTerminal(taskStatus?.status) ? (
                  <Button type="button" variant="ghost" size="sm" onClick={handleDismissResult}>
                    {t('common.close')}
                  </Button>
                ) : null}
                {taskCompleted && hasResultTarget ? (
                  <Button type="button" size="sm" onClick={handleOpenResult}>
                    {t('personalResearch.openThesis')}
                    <ArrowRight className="h-4 w-4" aria-hidden="true" />
                  </Button>
                ) : null}
              </div>
            </section>
          ) : (
            <>
              <div>
                <p className="text-sm font-semibold text-foreground">{t('personalResearch.stock')}</p>
                <p className="mt-1 rounded-xl border border-border/70 bg-surface px-3 py-2 font-mono text-sm text-foreground">
                  {normalizedStockCode || t('personalResearch.stockMissing')}
                </p>
                {!validStockCode ? (
                  <p className="mt-2 text-xs text-danger">{t('personalResearch.cnOnly')}</p>
                ) : null}
              </div>

              <fieldset>
                <legend className="text-sm font-semibold text-foreground">{t('personalResearch.modeTitle')}</legend>
                <div className="mt-3 grid gap-2 sm:grid-cols-2">
                  {MODE_OPTIONS.map((option) => {
                    const selected = mode === option.mode;
                    return (
                      <button
                        key={option.mode}
                        type="button"
                        role="radio"
                        aria-checked={selected}
                        onClick={() => handleModeChange(option.mode)}
                        className={`rounded-xl border p-3 text-left transition-colors ${selected ? 'border-cyan/60 bg-cyan/10' : 'border-border/70 bg-surface hover:bg-hover'}`}
                      >
                        <span className="block text-sm font-semibold text-foreground">{t(option.labelKey)}</span>
                        <span className="mt-1 block text-xs leading-5 text-secondary-text">{t(option.descriptionKey)}</span>
                      </button>
                    );
                  })}
                </div>
              </fieldset>

              {submitError ? (
                <ApiErrorAlert
                  error={{ ...submitError, title: t('personalResearch.submitFailed') }}
                />
              ) : null}

              <Button
                type="button"
                size="lg"
                isLoading={submitting}
                loadingText={t('personalResearch.submitting')}
                disabled={!validStockCode}
                onClick={() => void handleSubmit()}
                className="w-full"
              >
                <BrainCircuit className="h-4 w-4" aria-hidden="true" />
                {t('personalResearch.submit')}
              </Button>
            </>
          )}
        </div>
      </Drawer>, document.body)}
    </>
  );
};
