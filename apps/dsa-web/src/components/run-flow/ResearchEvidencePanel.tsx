import type React from 'react';
import { useMemo, useState } from 'react';
import {
  BookOpenCheck,
  ChevronDown,
  ExternalLink,
  FileSearch,
  Link2,
  RefreshCw,
} from 'lucide-react';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import { useResearchEvidence } from '../../hooks/useResearchEvidence';
import type {
  ResearchEvidenceCitation,
  ResearchEvidenceClaimStatus,
  ResearchEvidenceStatus,
} from '../../types/research';
import { cn } from '../../utils/cn';
import { Badge, Button, EmptyState, InlineAlert } from '../common';

interface ResearchEvidencePanelProps {
  taskId?: string | null;
}

const STATUS_VARIANT: Record<ResearchEvidenceStatus, 'success' | 'warning' | 'default' | 'danger'> = {
  available: 'success',
  partial: 'warning',
  empty: 'default',
  fetch_failed: 'danger',
};

const CLAIM_VARIANT: Record<ResearchEvidenceClaimStatus, 'success' | 'warning' | 'danger' | 'default'> = {
  supported: 'success',
  partial: 'warning',
  contradicted: 'danger',
  insufficient: 'default',
};

function externalHttpUrl(value: string | null): string | null {
  if (!value) {
    return null;
  }
  try {
    const parsed = new URL(value);
    return parsed.protocol === 'http:' || parsed.protocol === 'https:'
      ? parsed.href
      : null;
  } catch {
    return null;
  }
}

const compactHash = (value: string): string => (
  value.length > 18 ? `${value.slice(0, 12)}…${value.slice(-6)}` : value
);

const CitationItem: React.FC<{ citation: ResearchEvidenceCitation }> = ({ citation }) => {
  const { t } = useUiLanguage();
  const safeUrl = externalHttpUrl(citation.canonicalUrl);

  return (
    <li className="rounded-lg border border-border/50 bg-elevated/40 px-3 py-2" data-testid={`research-evidence-citation-${citation.id}`}>
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Link2 className="h-3.5 w-3.5 shrink-0 text-cyan" aria-hidden="true" />
        <span className="min-w-0 break-words text-xs font-medium text-foreground">
          {citation.title}
        </span>
        <Badge variant="default" className="shadow-none">
          {t(`researchEvidence.relation.${citation.relation}`)}
        </Badge>
      </div>
      <p className="mt-1 break-words text-xs text-secondary-text">{citation.excerpt}</p>
      <div className="mt-2 flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-muted-text">
        <span>{citation.sourceName}</span>
        <span aria-label={citation.artifactHash}>{compactHash(citation.artifactHash)}</span>
        {safeUrl ? (
          <a
            href={safeUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex min-w-0 items-center gap-1 break-all text-cyan hover:underline"
          >
            {t('researchEvidence.openSource')}
            <ExternalLink className="h-3 w-3 shrink-0" aria-hidden="true" />
          </a>
        ) : null}
      </div>
    </li>
  );
};

export const ResearchEvidencePanel: React.FC<ResearchEvidencePanelProps> = ({ taskId }) => {
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
  } = useResearchEvidence({ taskId, enabled: isOpen });

  const normalizedTaskId = taskId?.trim() || '';
  const expandedHashes = expandedState.taskId === normalizedTaskId
    ? expandedState.hashes
    : new Set<string>();
  const totalClaims = useMemo(
    () => items.reduce((sum, item) => sum + item.claimCount, 0),
    [items],
  );

  const toggleEvidence = (evidenceHash: string) => {
    const willOpen = !expandedHashes.has(evidenceHash);
    setExpandedState((current) => {
      const next = new Set(
        current.taskId === normalizedTaskId ? current.hashes : [],
      );
      if (next.has(evidenceHash)) {
        next.delete(evidenceHash);
      } else {
        next.add(evidenceHash);
      }
      return { taskId: normalizedTaskId, hashes: next };
    });
    if (willOpen) {
      void loadDetail(evidenceHash);
    }
  };

  return (
    <section
      className="overflow-hidden rounded-2xl border border-subtle bg-card/60"
      data-testid="research-evidence-panel"
    >
      <button
        type="button"
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-hover"
        aria-expanded={isOpen}
        onClick={() => setIsOpen((value) => !value)}
      >
        <span className="flex min-w-0 items-center gap-3">
          <BookOpenCheck className="h-4 w-4 shrink-0 text-cyan" aria-hidden="true" />
          <span className="min-w-0">
            <span className="block text-sm font-medium text-foreground">
              {t('researchEvidence.title')}
            </span>
            <span className="block truncate text-xs text-muted-text">
              {isOpen && items.length > 0
                ? t('researchEvidence.loadedSummary', {
                    count: items.length,
                    claims: totalClaims,
                  })
                : t('researchEvidence.lazyDescription')}
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
              title={t('researchEvidence.noTaskTitle')}
              description={t('researchEvidence.noTaskDescription')}
              icon={<FileSearch className="h-5 w-5" aria-hidden="true" />}
              className="bg-transparent py-6 shadow-none"
            />
          ) : isLoading ? (
            <div className="flex items-center gap-2 py-4 text-sm text-secondary-text" data-testid="research-evidence-loading">
              <div className="home-spinner h-5 w-5 animate-spin border-2" aria-hidden="true" />
              {t('researchEvidence.loading')}
            </div>
          ) : error ? (
            <InlineAlert
              variant="danger"
              title={t('researchEvidence.errorTitle')}
              message={error.message}
              action={(
                <Button type="button" variant="secondary" size="sm" onClick={refetch}>
                  <RefreshCw className="h-4 w-4" aria-hidden="true" />
                  {t('researchEvidence.retry')}
                </Button>
              )}
            />
          ) : items.length === 0 ? (
            <EmptyState
              title={t('researchEvidence.emptyTitle')}
              description={t('researchEvidence.emptyDescription')}
              icon={<FileSearch className="h-5 w-5" aria-hidden="true" />}
              className="bg-transparent py-6 shadow-none"
            />
          ) : (
            <div className="space-y-3">
              {items.map((item) => {
                const expanded = expandedHashes.has(item.evidenceHash);
                const detailState = details[item.evidenceHash];
                const detail = detailState?.detail;
                const citationById = new Map(
                  detail?.evidence.citations.map((citation) => [citation.id, citation]) || [],
                );

                return (
                  <article
                    key={item.evidenceHash}
                    className="overflow-hidden rounded-xl border border-border/60 bg-elevated/30"
                    data-testid={`research-evidence-item-${item.evidenceHash}`}
                  >
                    <button
                      type="button"
                      className="w-full px-3 py-3 text-left hover:bg-hover"
                      aria-expanded={expanded}
                      onClick={() => toggleEvidence(item.evidenceHash)}
                    >
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={STATUS_VARIANT[item.status]} className="shadow-none">
                          {t(`researchEvidence.status.${item.status}`)}
                        </Badge>
                        <span className="text-xs text-secondary-text">
                          {t('researchEvidence.coverage', {
                            value: Math.round(item.coverage * 100),
                          })}
                        </span>
                        <span className="text-xs text-secondary-text">
                          {t('researchEvidence.counts', {
                            claims: item.claimCount,
                            citations: item.citationCount,
                          })}
                        </span>
                        <code
                          className="ml-auto max-w-full break-all text-[11px] text-muted-text"
                          aria-label={item.evidenceHash}
                        >
                          {compactHash(item.evidenceHash)}
                        </code>
                      </div>
                    </button>

                    {expanded ? (
                      <div className="space-y-3 border-t border-border/50 px-3 py-3">
                        {detailState?.isLoading ? (
                          <p className="text-xs text-secondary-text">
                            {t('researchEvidence.detailLoading')}
                          </p>
                        ) : detailState?.error ? (
                          <InlineAlert
                            variant="danger"
                            title={t('researchEvidence.detailErrorTitle')}
                            message={detailState.error.message}
                            action={(
                              <Button
                                type="button"
                                variant="secondary"
                                size="sm"
                                onClick={() => void loadDetail(item.evidenceHash, true)}
                              >
                                {t('researchEvidence.retry')}
                              </Button>
                            )}
                          />
                        ) : detail ? (
                          <>
                            {detail.evidence.limitations.length > 0 ? (
                              <div className="rounded-lg border border-warning/20 bg-warning/5 px-3 py-2">
                                <p className="text-xs font-medium text-foreground">
                                  {t('researchEvidence.limitations')}
                                </p>
                                <ul className="mt-1 list-disc space-y-1 pl-4 text-xs text-secondary-text">
                                  {detail.evidence.limitations.map((limitation, index) => (
                                    <li key={`${item.evidenceHash}-limitation-${index}`} className="break-words">
                                      {limitation}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                            ) : null}

                            {detail.evidence.claims.length === 0 ? (
                              <p className="text-xs text-secondary-text">
                                {t('researchEvidence.noClaims')}
                              </p>
                            ) : (
                              <div className="space-y-2">
                                {detail.evidence.claims.map((claim) => {
                                  const citations = claim.citationIds
                                    .map((id) => citationById.get(id))
                                    .filter((citation): citation is ResearchEvidenceCitation => Boolean(citation));
                                  return (
                                    <details
                                      key={claim.id}
                                      className="rounded-lg border border-border/50 bg-card/60 px-3 py-2"
                                    >
                                      <summary className="cursor-pointer list-none text-sm text-foreground">
                                        <span className="flex min-w-0 flex-wrap items-center gap-2">
                                          <Badge variant={CLAIM_VARIANT[claim.status]} className="shadow-none">
                                            {t(`researchEvidence.claimStatus.${claim.status}`)}
                                          </Badge>
                                          <span className="min-w-0 flex-1 break-words">
                                            {claim.statement}
                                          </span>
                                          <span className="text-xs text-muted-text">
                                            {t('researchEvidence.citationCount', { count: citations.length })}
                                          </span>
                                        </span>
                                      </summary>
                                      <div className="mt-3 space-y-2 border-t border-border/40 pt-3">
                                        {claim.limitations.length > 0 ? (
                                          <ul className="list-disc space-y-1 pl-4 text-xs text-secondary-text">
                                            {claim.limitations.map((limitation, index) => (
                                              <li key={`${claim.id}-limitation-${index}`} className="break-words">
                                                {limitation}
                                              </li>
                                            ))}
                                          </ul>
                                        ) : null}
                                        {citations.length > 0 ? (
                                          <ul className="space-y-2">
                                            {citations.map((citation) => (
                                              <CitationItem key={citation.id} citation={citation} />
                                            ))}
                                          </ul>
                                        ) : (
                                          <p className="text-xs text-muted-text">
                                            {t('researchEvidence.noCitations')}
                                          </p>
                                        )}
                                      </div>
                                    </details>
                                  );
                                })}
                              </div>
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
                  title={t('researchEvidence.loadMoreErrorTitle')}
                  message={loadMoreError.message}
                  action={(
                    <Button type="button" variant="secondary" size="sm" onClick={() => void loadMore()}>
                      {t('researchEvidence.retry')}
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
                  loadingText={t('researchEvidence.loadingMore')}
                >
                  {t('researchEvidence.loadMore')}
                </Button>
              ) : null}
            </div>
          )}
        </div>
      ) : null}
    </section>
  );
};
