import { render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { researchApi } from '../../../api/research';
import { UiLanguageProvider } from '../../../contexts/UiLanguageContext';
import type {
  PersonalResearchDebateReviewResponse,
  PersonalResearchSkillExecutionResponse,
  PersonalResearchSkillId,
  PersonalResearchThesisResponse,
} from '../../../types/research';
import { PersonalResearchThesisPanel } from '../PersonalResearchThesisPanel';

vi.mock('../../../api/research', () => ({
  researchApi: {
    getLatestPersonalResearchThesisBySignal: vi.fn(),
    listPersonalResearchSkillExecutions: vi.fn(),
    getPersonalResearchDebateReview: vi.fn(),
  },
}));

const SKILL_IDS: PersonalResearchSkillId[] = [
  'personal-value-quality',
  'personal-trend-timing',
  'personal-catalyst',
  'personal-risk',
  'personal-evidence-quality',
];

const HASHES = SKILL_IDS.reduce<Record<PersonalResearchSkillId, string>>((result, skillId, index) => {
  result[skillId] = String(index + 1).repeat(64);
  return result;
}, {} as Record<PersonalResearchSkillId, string>);

const thesis: PersonalResearchThesisResponse = {
  contract: 'personal-research-thesis',
  version: 'personal-research-thesis-v1',
  thesisHash: 'a'.repeat(64),
  contentHash: 'b'.repeat(64),
  lineage: {
    taskId: 'task-42',
    market: 'cn',
    stockCode: '600519',
    researchSnapshotHash: 'c'.repeat(64),
    skillExecutionHashes: HASHES,
    debateSnapshotHash: 'd'.repeat(64),
    debateReviewHash: 'e'.repeat(64),
    decisionSignalId: 42,
    policyEvaluationHash: null,
    policyVersion: null,
    policyHash: null,
    portfolioSnapshotRef: null,
    supersedesThesisHash: null,
  },
  stance: 'bullish',
  accountAction: 'open_candidate',
  scores: {
    valueQualityScore: 70,
    trendTimingScore: 71,
    catalystScore: 72,
    riskScore: 73,
    evidenceQualityScore: 74,
  },
  catalysts: ['Earnings inflection'],
  invalidators: ['Margin decline'],
  unknowns: ['Forward guidance'],
  evidenceRefs: ['citation-1'],
  content: { contract: 'formal-thesis-content-v1' },
  createdAt: '2026-08-10T08:00:00Z',
};

function makeExecution(skillId: PersonalResearchSkillId, score: number): PersonalResearchSkillExecutionResponse {
  const scoreField = {
    'personal-value-quality': 'value_quality_score',
    'personal-trend-timing': 'trend_timing_score',
    'personal-catalyst': 'catalyst_score',
    'personal-risk': 'risk_score',
    'personal-evidence-quality': 'evidence_quality_score',
  }[skillId];
  return {
    contract: 'personal-research-skill-execution',
    version: 'v1',
    executionHash: HASHES[skillId],
    skillContract: {
      skillId,
      version: `${skillId}-v1`,
      contractHash: 'f'.repeat(64),
      scoreField,
    },
    lineage: {
      taskId: 'task-42',
      market: 'cn',
      stockCode: '600519',
      researchSnapshotHash: 'c'.repeat(64),
      factorSnapshotHash: '6'.repeat(64),
      evidenceSnapshotHash: '7'.repeat(64),
      datasetSnapshotHashes: ['8'.repeat(64)],
      datasetLineageHash: '9'.repeat(64),
      inputHash: 'a'.repeat(64),
      outputHash: 'b'.repeat(64),
    },
    input: { contract: 'input-v1' },
    result: { status: 'succeeded', score, output: { evidence_refs: ['citation-1'] } },
    createdAt: '2026-08-10T08:00:00Z',
  };
}

const executions = SKILL_IDS.map((skillId, index) => makeExecution(skillId, 70 + index));

const review: PersonalResearchDebateReviewResponse = {
  contract: 'personal-research-debate-review',
  version: 'v1',
  reviewHash: 'e'.repeat(64),
  lineage: {
    taskId: 'task-42',
    market: 'cn',
    stockCode: '600519',
    debateSnapshotHash: 'd'.repeat(64),
    evidenceSnapshotHash: '7'.repeat(64),
  },
  verifier: {
    version: 'verifier-v1',
    inputHash: '1'.repeat(64),
    outputHash: '2'.repeat(64),
    valid: true,
    failClosed: false,
    reasonCodes: ['citations_complete'],
    input: {},
    output: {},
  },
  judge: {
    version: 'judge-v1',
    policyHash: '3'.repeat(64),
    inputHash: '4'.repeat(64),
    outputHash: '5'.repeat(64),
    failClosed: false,
    reasonCodes: ['balanced_evidence'],
    verdict: 'balanced',
    winner: null,
    input: {},
    output: {},
  },
  createdAt: '2026-08-10T08:00:00Z',
};

function renderPanel(signalId = 42) {
  return render(
    <UiLanguageProvider>
      <PersonalResearchThesisPanel signalId={signalId} />
    </UiLanguageProvider>,
  );
}

beforeEach(() => {
  window.localStorage.clear();
  window.localStorage.setItem('dsa.uiLanguage', 'zh');
  vi.clearAllMocks();
  vi.mocked(researchApi.getLatestPersonalResearchThesisBySignal).mockResolvedValue(thesis);
  vi.mocked(researchApi.listPersonalResearchSkillExecutions).mockResolvedValue({
    contract: 'personal-research-skill-execution-collection',
    version: 'v1',
    lineage: { taskId: 'task-42', market: 'cn', stockCode: '600519' },
    expectedSkillIds: SKILL_IDS,
    missingSkillIds: [],
    complete: true,
    executions,
  });
  vi.mocked(researchApi.getPersonalResearchDebateReview).mockResolvedValue(review);
});

describe('PersonalResearchThesisPanel', () => {
  it('loads Thesis by signal and follows its exact Skill and Debate review lineage', async () => {
    renderPanel();

    expect(await screen.findByText('Personal Research Thesis')).toBeInTheDocument();
    await waitFor(() => {
      expect(researchApi.getLatestPersonalResearchThesisBySignal).toHaveBeenCalledWith(42);
      expect(researchApi.listPersonalResearchSkillExecutions).toHaveBeenCalledWith(
        'task-42',
        'cn',
        '600519',
      );
      expect(researchApi.getPersonalResearchDebateReview).toHaveBeenCalledWith('e'.repeat(64));
    });

    expect(screen.getByText(/legacy Skill Outcome \/ Decision Outcome v1/)).toBeInTheDocument();
    const valueSkill = screen.getByTestId('personal-research-skill-personal-value-quality');
    expect(within(valueSkill).getByText('70')).toBeInTheDocument();
    expect(within(valueSkill).getByText('personal-value-quality-v1')).toBeInTheDocument();
    expect(within(valueSkill).getByTitle(HASHES['personal-value-quality'])).toBeInTheDocument();
    expect(screen.getByText('已通过')).toBeInTheDocument();
    expect(screen.getAllByText('均衡').length).toBeGreaterThan(0);
    expect(screen.getByText('citations_complete')).toBeInTheDocument();
    expect(screen.getByText('balanced_evidence')).toBeInTheDocument();
    expect(screen.getByText('Earnings inflection')).toBeInTheDocument();
    expect(screen.getByText('Margin decline')).toBeInTheDocument();
    expect(screen.getByText('Forward guidance')).toBeInTheDocument();
    expect(screen.getByText('citation-1')).toBeInTheDocument();
  });

  it('treats a 404 as no formal Thesis and does not invent zero scores', async () => {
    vi.mocked(researchApi.getLatestPersonalResearchThesisBySignal).mockRejectedValue({
      response: {
        status: 404,
        data: { detail: { error: 'not_found', message: 'missing' } },
      },
    });

    renderPanel(99);

    expect(await screen.findByText('尚无正式 Thesis')).toBeInTheDocument();
    expect(screen.getByText(/尚未关联不可变的 Personal Research Thesis/)).toBeInTheDocument();
    expect(researchApi.listPersonalResearchSkillExecutions).not.toHaveBeenCalled();
    expect(researchApi.getPersonalResearchDebateReview).not.toHaveBeenCalled();
    expect(screen.queryByText('0')).not.toBeInTheDocument();
  });

  it('shows that Debate was not triggered when the Thesis has no review lineage', async () => {
    vi.mocked(researchApi.getLatestPersonalResearchThesisBySignal).mockResolvedValue({
      ...thesis,
      lineage: {
        ...thesis.lineage,
        debateSnapshotHash: null,
        debateReviewHash: null,
      },
    });

    renderPanel();

    expect(await screen.findByText('本次 Thesis 没有关联 Debate Review。')).toBeInTheDocument();
    expect(researchApi.getPersonalResearchDebateReview).not.toHaveBeenCalled();
  });

  it('keeps the formal Thesis visible when a related lineage read fails', async () => {
    vi.mocked(researchApi.listPersonalResearchSkillExecutions).mockRejectedValue({
      response: { status: 500, data: { detail: { message: 'skill storage unavailable' } } },
    });

    renderPanel();

    expect(await screen.findByText('看多')).toBeInTheDocument();
    expect(await screen.findByText(/skill storage unavailable/)).toBeInTheDocument();
    expect(screen.getAllByText('执行详情未读取')).toHaveLength(5);
    expect(screen.getByTestId('personal-research-skill-personal-value-quality'))
      .toHaveTextContent('70');
  });
});
