import type React from 'react';
import { useEffect, useMemo, useState } from 'react';
import { Badge, InlineAlert } from '../common';
import { researchApi } from '../../api/research';
import { getParsedApiError } from '../../api/error';
import { useUiLanguage } from '../../contexts/UiLanguageContext';
import type { UiLanguage, UiTextKey } from '../../i18n/uiText';
import type {
  PersonalResearchAccountAction,
  PersonalResearchDebateReviewResponse,
  PersonalResearchDebateVerdict,
  PersonalResearchSkillExecutionResponse,
  PersonalResearchSkillId,
  PersonalResearchThesisResponse,
  PersonalResearchThesisScores,
  PersonalResearchThesisStance,
} from '../../types/research';

type PersonalResearchThesisPanelProps = {
  signalId: number;
};

type LoadStatus = 'loading' | 'empty' | 'ready' | 'error';

type SkillDefinition = {
  id: PersonalResearchSkillId;
  labelKey: UiTextKey;
  scoreKey: keyof PersonalResearchThesisScores;
};

const SKILLS: SkillDefinition[] = [
  {
    id: 'personal-value-quality',
    labelKey: 'personalResearchThesis.skill.valueQuality',
    scoreKey: 'valueQualityScore',
  },
  {
    id: 'personal-trend-timing',
    labelKey: 'personalResearchThesis.skill.trendTiming',
    scoreKey: 'trendTimingScore',
  },
  {
    id: 'personal-catalyst',
    labelKey: 'personalResearchThesis.skill.catalyst',
    scoreKey: 'catalystScore',
  },
  {
    id: 'personal-risk',
    labelKey: 'personalResearchThesis.skill.risk',
    scoreKey: 'riskScore',
  },
  {
    id: 'personal-evidence-quality',
    labelKey: 'personalResearchThesis.skill.evidenceQuality',
    scoreKey: 'evidenceQualityScore',
  },
];

const STANCE_KEYS: Record<PersonalResearchThesisStance, UiTextKey> = {
  strong_bullish: 'personalResearchThesis.stance.strong_bullish',
  bullish: 'personalResearchThesis.stance.bullish',
  watch: 'personalResearchThesis.stance.watch',
  neutral: 'personalResearchThesis.stance.neutral',
  bearish: 'personalResearchThesis.stance.bearish',
  avoid: 'personalResearchThesis.stance.avoid',
};

const ACTION_KEYS: Record<PersonalResearchAccountAction, UiTextKey> = {
  observe: 'personalResearchThesis.action.observe',
  open_candidate: 'personalResearchThesis.action.open_candidate',
  add_candidate: 'personalResearchThesis.action.add_candidate',
  hold: 'personalResearchThesis.action.hold',
  reduce_candidate: 'personalResearchThesis.action.reduce_candidate',
  exit_candidate: 'personalResearchThesis.action.exit_candidate',
};

const VERDICT_KEYS: Record<PersonalResearchDebateVerdict, UiTextKey> = {
  bull: 'personalResearchThesis.verdict.bull',
  bear: 'personalResearchThesis.verdict.bear',
  balanced: 'personalResearchThesis.verdict.balanced',
  fail_closed: 'personalResearchThesis.verdict.fail_closed',
};

const LOCALE_BY_LANGUAGE: Record<UiLanguage, string> = {
  zh: 'zh-CN',
  en: 'en-US',
};

function compactHash(value: string | null | undefined): string {
  if (!value) return '—';
  if (value.length <= 20) return value;
  return `${value.slice(0, 10)}…${value.slice(-8)}`;
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return '—';
  return Number(value).toFixed(2).replace(/\.?0+$/, '');
}

function formatDateTime(value: string, language: UiLanguage): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  return new Intl.DateTimeFormat(LOCALE_BY_LANGUAGE[language], {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

const HashValue: React.FC<{ value: string | null | undefined }> = ({ value }) => (
  <code
    className="break-all font-mono text-[11px] text-secondary-text"
    title={value ?? undefined}
  >
    {compactHash(value)}
  </code>
);

const ArtifactList: React.FC<{ title: string; items: string[] }> = ({ title, items }) => (
  <section className="rounded-xl border border-border/55 bg-elevated/30 p-3">
    <h5 className="text-xs font-semibold uppercase tracking-wide text-muted-text">{title}</h5>
    {items.length > 0 ? (
      <ul className="mt-2 list-disc space-y-1 pl-4 text-sm leading-5 text-secondary-text">
        {items.map((item, index) => (
          <li key={`${title}-${index}`} className="break-words">{item}</li>
        ))}
      </ul>
    ) : (
      <p className="mt-2 text-sm text-muted-text">—</p>
    )}
  </section>
);

const ReasonCodes: React.FC<{
  title: string;
  reasonCodes: string[];
  emptyLabel: string;
}> = ({ title, reasonCodes, emptyLabel }) => (
  <div className="mt-3">
    <p className="text-[11px] font-medium text-muted-text">{title}</p>
    {reasonCodes.length > 0 ? (
      <div className="mt-1.5 flex flex-wrap gap-1.5">
        {reasonCodes.map((reason) => (
          <code
            key={reason}
            className="rounded-md border border-border/55 bg-background/50 px-2 py-1 text-[11px] text-secondary-text"
          >
            {reason}
          </code>
        ))}
      </div>
    ) : (
      <p className="mt-1 text-xs text-muted-text">{emptyLabel}</p>
    )}
  </div>
);

export const PersonalResearchThesisPanel: React.FC<PersonalResearchThesisPanelProps> = ({
  signalId,
}) => {
  const { language, t } = useUiLanguage();
  const [status, setStatus] = useState<LoadStatus>('loading');
  const [loadedSignalId, setLoadedSignalId] = useState(signalId);
  const [thesis, setThesis] = useState<PersonalResearchThesisResponse | null>(null);
  const [executions, setExecutions] = useState<PersonalResearchSkillExecutionResponse[]>([]);
  const [review, setReview] = useState<PersonalResearchDebateReviewResponse | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [reloadToken, setReloadToken] = useState(0);

  useEffect(() => {
    let cancelled = false;

    void researchApi.getLatestPersonalResearchThesisBySignal(signalId)
      .then(async (nextThesis) => {
        if (cancelled) return;
        setLoadedSignalId(signalId);
        setThesis(nextThesis);
        setExecutions([]);
        setReview(null);
        setErrorMessage(null);
        setDetailError(null);
        setStatus('ready');
        setDetailLoading(true);

        const detailErrors: string[] = [];
        const skillsPromise = researchApi.listPersonalResearchSkillExecutions(
          nextThesis.lineage.taskId,
          nextThesis.lineage.market,
          nextThesis.lineage.stockCode,
        ).then((response) => {
          if (!cancelled) setExecutions(response.executions);
        }).catch((error: unknown) => {
          detailErrors.push(getParsedApiError(error).message);
        });

        const reviewPromise = nextThesis.lineage.debateReviewHash
          ? researchApi.getPersonalResearchDebateReview(nextThesis.lineage.debateReviewHash)
            .then((response) => {
              if (!cancelled) setReview(response);
            })
            .catch((error: unknown) => {
              detailErrors.push(getParsedApiError(error).message);
            })
          : Promise.resolve();

        await Promise.all([skillsPromise, reviewPromise]);
        if (cancelled) return;
        setDetailError(detailErrors.length > 0 ? detailErrors.join('; ') : null);
        setDetailLoading(false);
      })
      .catch((error: unknown) => {
        if (cancelled) return;
        const parsed = getParsedApiError(error);
        setLoadedSignalId(signalId);
        setThesis(null);
        setExecutions([]);
        setReview(null);
        setDetailError(null);
        setDetailLoading(false);
        if (parsed.status === 404) {
          setErrorMessage(null);
          setStatus('empty');
          return;
        }
        setStatus('error');
        setErrorMessage(parsed.message);
      });

    return () => {
      cancelled = true;
    };
  }, [reloadToken, signalId]);

  const visibleStatus: LoadStatus = loadedSignalId === signalId ? status : 'loading';

  const executionsBySkill = useMemo(() => {
    const result = new Map<PersonalResearchSkillId, PersonalResearchSkillExecutionResponse>();
    if (!thesis) return result;
    for (const execution of executions) {
      const expectedHash = thesis.lineage.skillExecutionHashes[execution.skillContract.skillId];
      if (execution.executionHash === expectedHash) {
        result.set(execution.skillContract.skillId, execution);
      }
    }
    return result;
  }, [executions, thesis]);

  return (
    <section
      className="rounded-2xl border border-cyan/20 bg-cyan/5 p-4"
      data-testid="personal-research-thesis-panel"
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold text-foreground">
            {t('personalResearchThesis.title')}
          </h3>
          <p className="mt-1 text-xs leading-5 text-secondary-text">
            {t('personalResearchThesis.legacyBoundary')}
          </p>
        </div>
        <Badge variant="info">{t('personalResearchThesis.readOnly')}</Badge>
      </div>

      {visibleStatus === 'loading' ? (
        <p className="mt-4 text-sm text-secondary-text" role="status">
          {t('personalResearchThesis.loading')}
        </p>
      ) : null}

      {visibleStatus === 'empty' ? (
        <div className="mt-4 rounded-xl border border-dashed border-border/70 px-4 py-5 text-center">
          <p className="text-sm font-semibold text-foreground">{t('personalResearchThesis.empty')}</p>
          <p className="mt-1 text-xs leading-5 text-secondary-text">
            {t('personalResearchThesis.emptyDescription')}
          </p>
        </div>
      ) : null}

      {visibleStatus === 'error' ? (
        <InlineAlert
          className="mt-4"
          variant="danger"
          title={t('personalResearchThesis.loadError')}
          message={errorMessage ?? t('personalResearchThesis.loadError')}
          action={(
            <button
              type="button"
              className="btn-secondary !px-3 !py-1.5 !text-xs"
              onClick={() => {
                setStatus('loading');
                setReloadToken((current) => current + 1);
              }}
            >
              {t('common.retry')}
            </button>
          )}
        />
      ) : null}

      {visibleStatus === 'ready' && thesis ? (
        <div className="mt-4 space-y-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="rounded-xl border border-border/55 bg-elevated/35 p-3">
              <p className="text-xs text-muted-text">{t('personalResearchThesis.stance')}</p>
              <p className="mt-1 text-sm font-semibold text-foreground">{t(STANCE_KEYS[thesis.stance])}</p>
            </div>
            <div className="rounded-xl border border-border/55 bg-elevated/35 p-3">
              <p className="text-xs text-muted-text">{t('personalResearchThesis.accountAction')}</p>
              <p className="mt-1 text-sm font-semibold text-foreground">{t(ACTION_KEYS[thesis.accountAction])}</p>
            </div>
          </div>

          <section>
            <h4 className="text-sm font-semibold text-foreground">
              {t('personalResearchThesis.skillsTitle')}
            </h4>
            <div className="mt-2 grid gap-2 sm:grid-cols-2">
              {SKILLS.map((skill) => {
                const executionHash = thesis.lineage.skillExecutionHashes[skill.id];
                const execution = executionsBySkill.get(skill.id);
                return (
                  <article
                    key={skill.id}
                    className="rounded-xl border border-border/55 bg-elevated/30 p-3"
                    data-testid={`personal-research-skill-${skill.id}`}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-medium text-foreground">{t(skill.labelKey)}</p>
                        <p className="mt-0.5 font-mono text-[10px] text-muted-text">{skill.id}</p>
                      </div>
                      <span className="text-lg font-semibold tabular-nums text-cyan">
                        {formatScore(thesis.scores[skill.scoreKey])}
                      </span>
                    </div>
                    <dl className="mt-3 grid gap-2 text-xs">
                      <div className="flex items-start justify-between gap-3">
                        <dt className="text-muted-text">{t('personalResearchThesis.version')}</dt>
                        <dd className="text-right text-secondary-text">
                          {execution?.skillContract.version ?? '—'}
                        </dd>
                      </div>
                      <div className="flex items-start justify-between gap-3">
                        <dt className="text-muted-text">{t('personalResearchThesis.execution')}</dt>
                        <dd><HashValue value={executionHash} /></dd>
                      </div>
                      {execution ? (
                        <>
                          <div className="flex items-start justify-between gap-3">
                            <dt className="text-muted-text">{t('personalResearchThesis.skillStatus')}</dt>
                            <dd className="text-secondary-text">{execution.result.status}</dd>
                          </div>
                          <div className="flex items-start justify-between gap-3">
                            <dt className="text-muted-text">{t('personalResearchThesis.factorSnapshot')}</dt>
                            <dd><HashValue value={execution.lineage.factorSnapshotHash} /></dd>
                          </div>
                          <div className="flex items-start justify-between gap-3">
                            <dt className="text-muted-text">{t('personalResearchThesis.evidenceSnapshot')}</dt>
                            <dd><HashValue value={execution.lineage.evidenceSnapshotHash} /></dd>
                          </div>
                        </>
                      ) : !detailLoading ? (
                        <div className="text-warning">
                          {t('personalResearchThesis.executionDetailMissing')}
                        </div>
                      ) : null}
                    </dl>
                  </article>
                );
              })}
            </div>
          </section>

          {detailError ? (
            <InlineAlert
              variant="warning"
              message={t('personalResearchThesis.detailLoadError', { message: detailError })}
            />
          ) : null}

          <section>
            <h4 className="text-sm font-semibold text-foreground">
              {t('personalResearchThesis.debateTitle')}
            </h4>
            {review ? (
              <div className="mt-2 grid gap-3 sm:grid-cols-2">
                <article className="rounded-xl border border-border/55 bg-elevated/30 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-medium text-foreground">
                        {t('personalResearchThesis.verifier')}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-text">{review.verifier.version}</p>
                    </div>
                    <Badge variant={review.verifier.valid && !review.verifier.failClosed ? 'success' : 'danger'}>
                      {review.verifier.valid
                        ? t('personalResearchThesis.verifierValid')
                        : t('personalResearchThesis.verifierInvalid')}
                    </Badge>
                  </div>
                  {review.verifier.failClosed ? (
                    <p className="mt-2 text-xs font-medium text-danger">
                      {t('personalResearchThesis.failClosed')}
                    </p>
                  ) : null}
                  <ReasonCodes
                    title={t('personalResearchThesis.reasonCodes')}
                    reasonCodes={review.verifier.reasonCodes}
                    emptyLabel={t('personalResearchThesis.noReasonCodes')}
                  />
                </article>
                <article className="rounded-xl border border-border/55 bg-elevated/30 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-sm font-medium text-foreground">
                        {t('personalResearchThesis.judge')}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-text">{review.judge.version}</p>
                    </div>
                    <Badge variant={review.judge.failClosed ? 'danger' : 'info'}>
                      {t(VERDICT_KEYS[review.judge.verdict])}
                    </Badge>
                  </div>
                  <dl className="mt-3 space-y-1 text-xs">
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-text">{t('personalResearchThesis.verdict')}</dt>
                      <dd className="text-secondary-text">{t(VERDICT_KEYS[review.judge.verdict])}</dd>
                    </div>
                    <div className="flex justify-between gap-2">
                      <dt className="text-muted-text">{t('personalResearchThesis.winner')}</dt>
                      <dd className="text-secondary-text">
                        {review.judge.winner
                          ? t(VERDICT_KEYS[review.judge.winner])
                          : '—'}
                      </dd>
                    </div>
                  </dl>
                  <ReasonCodes
                    title={t('personalResearchThesis.reasonCodes')}
                    reasonCodes={review.judge.reasonCodes}
                    emptyLabel={t('personalResearchThesis.noReasonCodes')}
                  />
                </article>
              </div>
            ) : !detailLoading && !thesis.lineage.debateReviewHash ? (
              <p className="mt-2 rounded-xl border border-dashed border-border/70 px-3 py-3 text-sm text-secondary-text">
                {t('personalResearchThesis.debateNotTriggered')}
              </p>
            ) : detailLoading ? (
              <p className="mt-2 text-sm text-secondary-text">{t('common.loading')}...</p>
            ) : null}
          </section>

          <div className="grid gap-3 sm:grid-cols-2">
            <ArtifactList title={t('personalResearchThesis.catalysts')} items={thesis.catalysts} />
            <ArtifactList title={t('personalResearchThesis.invalidators')} items={thesis.invalidators} />
            <ArtifactList title={t('personalResearchThesis.unknowns')} items={thesis.unknowns} />
            <ArtifactList title={t('personalResearchThesis.evidenceRefs')} items={thesis.evidenceRefs} />
          </div>

          <section className="rounded-xl border border-border/55 bg-background/35 p-3">
            <h4 className="text-xs font-semibold uppercase tracking-wide text-muted-text">
              {t('personalResearchThesis.lineage')}
            </h4>
            <dl className="mt-2 grid gap-2 text-xs sm:grid-cols-2">
              <div>
                <dt className="text-muted-text">{t('personalResearchThesis.version')}</dt>
                <dd className="mt-0.5 break-words text-secondary-text">{thesis.version}</dd>
              </div>
              <div>
                <dt className="text-muted-text">{t('personalResearchThesis.task')}</dt>
                <dd className="mt-0.5 break-all font-mono text-secondary-text">
                  {thesis.lineage.taskId}
                </dd>
              </div>
              <div>
                <dt className="text-muted-text">{t('personalResearchThesis.thesisHash')}</dt>
                <dd className="mt-0.5"><HashValue value={thesis.thesisHash} /></dd>
              </div>
              <div>
                <dt className="text-muted-text">{t('personalResearchThesis.researchSnapshot')}</dt>
                <dd className="mt-0.5"><HashValue value={thesis.lineage.researchSnapshotHash} /></dd>
              </div>
              <div>
                <dt className="text-muted-text">DecisionSignal</dt>
                <dd className="mt-0.5 text-secondary-text">
                  {thesis.lineage.decisionSignalId ?? '—'}
                </dd>
              </div>
              <div>
                <dt className="text-muted-text">{t('personalResearchThesis.createdAt')}</dt>
                <dd className="mt-0.5 text-secondary-text">
                  {formatDateTime(thesis.createdAt, language)}
                </dd>
              </div>
            </dl>
          </section>
        </div>
      ) : null}
    </section>
  );
};
