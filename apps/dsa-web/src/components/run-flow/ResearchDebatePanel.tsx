import type React from 'react';
import { useMemo, useState } from 'react';
import {
  ChevronDown,
  CircleHelp,
  MessagesSquare,
  RefreshCw,
  Scale,
  TrendingDown,
  TrendingUp,
} from 'lucide-react';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import { useResearchDebates } from '../../hooks/useResearchDebates';
import type {
  ResearchDebateStatus,
  ResearchDebateTurn,
} from '../../types/research';
import { cn } from '../../utils/cn';
import { Badge, Button, EmptyState, InlineAlert } from '../common';

interface ResearchDebatePanelProps {
  taskId?: string | null;
}

const STATUS_VARIANT: Record<
  ResearchDebateStatus,
  'success' | 'warning' | 'default' | 'danger'
> = {
  available: 'success',
  partial: 'warning',
  empty: 'default',
  generation_failed: 'danger',
};

const compactHash = (value: string): string => (
  value.length > 18 ? `${value.slice(0, 12)}…${value.slice(-6)}` : value
);

const DebateTurnCard: React.FC<{ turn: ResearchDebateTurn }> = ({ turn }) => {
  const { t } = useUiLanguage();
  const isBull = turn.stance === 'bull';
  const Icon = isBull ? TrendingUp : TrendingDown;

  return (
    <section
      className="rounded-xl border border-border/60 bg-card/60 px-3 py-3"
      data-testid={`research-debate-turn-${turn.stance}`}
    >
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Icon
          className={cn('h-4 w-4 shrink-0', isBull ? 'text-success' : 'text-danger')}
          aria-hidden="true"
        />
        <h4 className="text-sm font-semibold text-foreground">
          {t(`researchDebate.stance.${turn.stance}`)}
        </h4>
        <Badge variant="default" className="shadow-none">
          {t('researchDebate.argumentCount', { count: turn.arguments.length })}
        </Badge>
      </div>

      <p className="mt-2 break-words text-sm text-secondary-text">{turn.summary}</p>

      {turn.arguments.length > 0 ? (
        <div className="mt-3 space-y-2">
          {turn.arguments.map((argument) => (
            <article
              key={argument.id}
              className="rounded-lg border border-border/50 bg-elevated/35 px-3 py-2"
              data-testid={`research-debate-argument-${argument.id}`}
            >
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <code className="break-all text-[11px] text-muted-text">{argument.id}</code>
                <span className="ml-auto text-xs text-secondary-text">
                  {t('researchDebate.confidence', {
                    value: Math.round(argument.confidence * 100),
                  })}
                </span>
              </div>
              <p className="mt-1 break-words text-sm text-foreground">
                {argument.statement}
              </p>
              <div className="mt-2 space-y-1 text-xs text-secondary-text">
                <div className="flex min-w-0 flex-wrap gap-1.5">
                  <span>{t('researchDebate.claimIds')}</span>
                  {argument.claimIds.map((claimId) => (
                    <code key={claimId} className="break-all rounded bg-card px-1.5 py-0.5">
                      {claimId}
                    </code>
                  ))}
                </div>
                <div className="flex min-w-0 flex-wrap gap-1.5">
                  <span>{t('researchDebate.citationIds')}</span>
                  {argument.citationIds.map((citationId) => (
                    <code key={citationId} className="break-all rounded bg-card px-1.5 py-0.5">
                      {citationId}
                    </code>
                  ))}
                </div>
              </div>
              {argument.limitations.length > 0 ? (
                <ul className="mt-2 list-disc space-y-1 pl-4 text-xs text-secondary-text">
                  {argument.limitations.map((limitation, index) => (
                    <li key={`${argument.id}-limitation-${index}`} className="break-words">
                      {limitation}
                    </li>
                  ))}
                </ul>
              ) : null}
            </article>
          ))}
        </div>
      ) : (
        <p className="mt-3 text-xs text-muted-text">
          {t('researchDebate.noArguments')}
        </p>
      )}

      {turn.openQuestions.length > 0 ? (
        <div className="mt-3 rounded-lg border border-warning/20 bg-warning/5 px-3 py-2">
          <p className="flex items-center gap-1.5 text-xs font-medium text-foreground">
            <CircleHelp className="h-3.5 w-3.5" aria-hidden="true" />
            {t('researchDebate.openQuestions')}
          </p>
          <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-secondary-text">
            {turn.openQuestions.map((question, index) => (
              <li key={`${turn.turnHash}-question-${index}`} className="break-words">
                {question}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <div className="mt-3 flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-[11px] text-muted-text">
        <span className="break-words">{turn.modelUsed}</span>
        <code className="break-all" aria-label={turn.turnHash}>
          {compactHash(turn.turnHash)}
        </code>
        <code className="break-all" aria-label={turn.promptFingerprint}>
          {compactHash(turn.promptFingerprint)}
        </code>
      </div>
    </section>
  );
};

export const ResearchDebatePanel: React.FC<ResearchDebatePanelProps> = ({ taskId }) => {
  const { t } = useUiLanguage();
  const [isOpen, setIsOpen] = useState(false);
  const [expandedState, setExpandedState] = useState<{
    taskId: string;
    hashes: Set<string>;
  }>(() => ({ taskId: '', hashes: new Set() }));
  const {
    items,
    nextCursor,
    isLoading,
    isLoadingMore,
    error,
    loadMoreError,
    details,
    refetch,
    loadMore,
    loadDetail,
  } = useResearchDebates({ taskId, enabled: isOpen });

  const normalizedTaskId = taskId?.trim() || '';
  const expandedHashes = expandedState.taskId === normalizedTaskId
    ? expandedState.hashes
    : new Set<string>();
  const totalArguments = useMemo(
    () => items.reduce(
      (sum, item) => sum + item.bullArgumentCount + item.bearArgumentCount,
      0,
    ),
    [items],
  );

  const toggleDebate = (debateHash: string) => {
    const willOpen = !expandedHashes.has(debateHash);
    setExpandedState((current) => {
      const next = new Set(
        current.taskId === normalizedTaskId ? current.hashes : [],
      );
      if (next.has(debateHash)) {
        next.delete(debateHash);
      } else {
        next.add(debateHash);
      }
      return { taskId: normalizedTaskId, hashes: next };
    });
    if (willOpen) {
      void loadDetail(debateHash);
    }
  };

  return (
    <section
      className="overflow-hidden rounded-2xl border border-subtle bg-card/60"
      data-testid="research-debate-panel"
    >
      <button
        type="button"
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-hover"
        aria-expanded={isOpen}
        onClick={() => setIsOpen((value) => !value)}
      >
        <span className="flex min-w-0 items-center gap-3">
          <Scale className="h-4 w-4 shrink-0 text-cyan" aria-hidden="true" />
          <span className="min-w-0">
            <span className="block text-sm font-medium text-foreground">
              {t('researchDebate.title')}
            </span>
            <span className="block truncate text-xs text-muted-text">
              {isOpen && items.length > 0
                ? t('researchDebate.loadedSummary', {
                    count: items.length,
                    arguments: totalArguments,
                  })
                : t('researchDebate.lazyDescription')}
            </span>
          </span>
        </span>
        <ChevronDown
          className={cn(
            'h-4 w-4 shrink-0 text-secondary-text transition-transform',
            isOpen && 'rotate-180',
          )}
          aria-hidden="true"
        />
      </button>

      {isOpen ? (
        <div className="space-y-3 border-t border-subtle px-4 py-3">
          {!normalizedTaskId ? (
            <EmptyState
              title={t('researchDebate.noTaskTitle')}
              description={t('researchDebate.noTaskDescription')}
              icon={<MessagesSquare className="h-5 w-5" aria-hidden="true" />}
              className="bg-transparent py-6 shadow-none"
            />
          ) : isLoading ? (
            <div className="flex items-center gap-2 py-4 text-sm text-secondary-text" data-testid="research-debate-loading">
              <div className="home-spinner h-5 w-5 animate-spin border-2" aria-hidden="true" />
              {t('researchDebate.loading')}
            </div>
          ) : error ? (
            <InlineAlert
              variant="danger"
              title={t('researchDebate.errorTitle')}
              message={error.message}
              action={(
                <Button type="button" variant="secondary" size="sm" onClick={refetch}>
                  <RefreshCw className="h-4 w-4" aria-hidden="true" />
                  {t('researchDebate.retry')}
                </Button>
              )}
            />
          ) : items.length === 0 ? (
            <EmptyState
              title={t('researchDebate.emptyTitle')}
              description={t('researchDebate.emptyDescription')}
              icon={<MessagesSquare className="h-5 w-5" aria-hidden="true" />}
              className="bg-transparent py-6 shadow-none"
            />
          ) : (
            <div className="space-y-3">
              {items.map((item) => {
                const expanded = expandedHashes.has(item.debateHash);
                const detailState = details[item.debateHash];
                const detail = detailState?.detail;

                return (
                  <article
                    key={item.debateHash}
                    className="overflow-hidden rounded-xl border border-border/60 bg-elevated/30"
                    data-testid={`research-debate-item-${item.debateHash}`}
                  >
                    <button
                      type="button"
                      className="w-full px-3 py-3 text-left hover:bg-hover"
                      aria-expanded={expanded}
                      onClick={() => toggleDebate(item.debateHash)}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={STATUS_VARIANT[item.status]} className="shadow-none">
                          {t(`researchDebate.status.${item.status}`)}
                        </Badge>
                        <span className="text-xs text-secondary-text">
                          {t('researchDebate.counts', {
                            bull: item.bullArgumentCount,
                            bear: item.bearArgumentCount,
                            questions: item.openQuestionCount,
                          })}
                        </span>
                        <code
                          className="ml-auto max-w-full break-all text-[11px] text-muted-text"
                          aria-label={item.debateHash}
                        >
                          {compactHash(item.debateHash)}
                        </code>
                      </div>
                    </button>

                    {expanded ? (
                      <div className="space-y-3 border-t border-border/50 px-3 py-3">
                        {detailState?.isLoading ? (
                          <p className="text-xs text-secondary-text">
                            {t('researchDebate.detailLoading')}
                          </p>
                        ) : detailState?.error ? (
                          <InlineAlert
                            variant="danger"
                            title={t('researchDebate.detailErrorTitle')}
                            message={detailState.error.message}
                            action={(
                              <Button
                                type="button"
                                variant="secondary"
                                size="sm"
                                onClick={() => void loadDetail(item.debateHash, true)}
                              >
                                {t('researchDebate.retry')}
                              </Button>
                            )}
                          />
                        ) : detail ? (
                          <>
                            {detail.debate.failedStances.length > 0 ? (
                              <InlineAlert
                                variant="warning"
                                title={t('researchDebate.failedStances')}
                                message={detail.debate.failedStances
                                  .map((failure) => (
                                    `${t(`researchDebate.stance.${failure.stance}`)} (${failure.errorCode})`
                                  ))
                                  .join(', ')}
                              />
                            ) : null}
                            {detail.debate.limitations.length > 0 ? (
                              <div className="rounded-lg border border-warning/20 bg-warning/5 px-3 py-2">
                                <p className="text-xs font-medium text-foreground">
                                  {t('researchDebate.limitations')}
                                </p>
                                <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-secondary-text">
                                  {detail.debate.limitations.map((limitation, index) => (
                                    <li key={`${item.debateHash}-limitation-${index}`} className="break-words">
                                      {limitation}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                            ) : null}
                            {detail.debate.turns.length > 0 ? (
                              <div className="grid gap-3 lg:grid-cols-2">
                                {detail.debate.turns.map((turn) => (
                                  <DebateTurnCard key={turn.turnHash} turn={turn} />
                                ))}
                              </div>
                            ) : (
                              <p className="text-xs text-secondary-text">
                                {t('researchDebate.noTurns')}
                              </p>
                            )}
                          </>
                        ) : null}
                      </div>
                    ) : null}
                  </article>
                );
              })}

              {loadMoreError ? (
                <InlineAlert
                  variant="danger"
                  title={t('researchDebate.loadMoreErrorTitle')}
                  message={loadMoreError.message}
                  action={(
                    <Button type="button" variant="secondary" size="sm" onClick={() => void loadMore()}>
                      {t('researchDebate.retry')}
                    </Button>
                  )}
                />
              ) : null}
              {nextCursor ? (
                <Button
                  type="button"
                  variant="secondary"
                  size="sm"
                  onClick={() => void loadMore()}
                  isLoading={isLoadingMore}
                  loadingText={t('researchDebate.loadingMore')}
                >
                  {t('researchDebate.loadMore')}
                </Button>
              ) : null}
            </div>
          )}
        </div>
      ) : null}
    </section>
  );
};
