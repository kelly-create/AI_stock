import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { researchApi } from '../../../api/research';
import type {
  ResearchDebateDetailResponse,
  ResearchDebateSummary,
} from '../../../types/research';
import { ResearchDebatePanel } from '../ResearchDebatePanel';

vi.mock('../../../api/research', () => ({
  researchApi: {
    listDebates: vi.fn(),
    getDebate: vi.fn(),
  },
}));

const HASH_ONE = 'a'.repeat(64);
const HASH_TWO = 'b'.repeat(64);
const EVIDENCE_HASH = 'c'.repeat(64);
const REQUEST_HASH = 'd'.repeat(64);
const BULL_TURN_HASH = 'e'.repeat(64);
const BEAR_TURN_HASH = 'f'.repeat(64);
const PROMPT_FINGERPRINT = '1'.repeat(64);

const summary = (
  debateHash: string,
  stockCode = '600519',
): ResearchDebateSummary => ({
  id: debateHash === HASH_ONE ? 1 : 2,
  stockCode,
  market: 'cn',
  debateEngineVersion: 'research-debate-v1',
  outputSchemaVersion: 'research-debate-output-v1',
  promptVersion: 'research-debate-prompt-v1',
  evidenceSnapshotHash: EVIDENCE_HASH,
  requestHash: REQUEST_HASH,
  modelRouteFingerprint: '2'.repeat(64),
  asOf: '2026-08-08T08:00:00Z',
  availableAt: '2026-08-08T08:00:00Z',
  status: 'available',
  bullTurnHash: BULL_TURN_HASH,
  bearTurnHash: BEAR_TURN_HASH,
  bullArgumentCount: 1,
  bearArgumentCount: 1,
  openQuestionCount: 1,
  debateHash,
  originJobId: 'task-1',
  createdAt: '2026-08-08T08:00:00Z',
});

const detail = (debateHash = HASH_ONE): ResearchDebateDetailResponse => ({
  ...summary(debateHash),
  debate: {
    debateEngineVersion: 'research-debate-v1',
    outputSchemaVersion: 'research-debate-output-v1',
    promptVersion: 'research-debate-prompt-v1',
    stockCode: '600519',
    market: 'cn',
    asOf: '2026-08-08T08:00:00Z',
    availableAt: '2026-08-08T08:00:00Z',
    status: 'available',
    evidenceSnapshotHash: EVIDENCE_HASH,
    requestHash: REQUEST_HASH,
    modelRouteFingerprint: '2'.repeat(64),
    bullTurnHash: BULL_TURN_HASH,
    bearTurnHash: BEAR_TURN_HASH,
    failedStances: [],
    limitations: ['Derived model output is not new evidence.'],
    turns: [
      {
        turnHash: BULL_TURN_HASH,
        promptFingerprint: PROMPT_FINGERPRINT,
        modelUsed: 'provider/model',
        stance: 'bull',
        summary: '<script>window.__debateXss=true</script> Bull summary.',
        arguments: [{
          id: 'bull-1',
          statement: '<img src=x onerror="window.__debateXss=true"> Value supports upside.',
          claimIds: ['claim-1'],
          citationIds: ['citation-1'],
          confidence: 0.7,
          limitations: ['One reporting period.'],
        }],
        openQuestions: ['Could <svg onload="window.__debateXss=true"> persist?'],
      },
      {
        turnHash: BEAR_TURN_HASH,
        promptFingerprint: PROMPT_FINGERPRINT,
        modelUsed: 'provider/model',
        stance: 'bear',
        summary: 'History remains incomplete.',
        arguments: [{
          id: 'bear-1',
          statement: 'Incomplete history limits confidence.',
          claimIds: ['claim-1'],
          citationIds: ['citation-1'],
          confidence: 0.6,
          limitations: [],
        }],
        openQuestions: [],
      },
    ],
  },
});

describe('ResearchDebatePanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('loads lazily and renders bounded turns as plain text without native titles', async () => {
    vi.mocked(researchApi.listDebates).mockResolvedValue({
      items: [summary(HASH_ONE)],
      count: 1,
      nextCursor: null,
    });
    vi.mocked(researchApi.getDebate).mockResolvedValue(detail());
    const { container } = render(<ResearchDebatePanel taskId="task-1" />);

    expect(researchApi.listDebates).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: /研究辩论/ }));

    expect(await screen.findByText('多方 1 / 空方 1 / 未决 1')).toBeInTheDocument();
    expect(researchApi.listDebates).toHaveBeenCalledWith({
      jobId: 'task-1',
      limit: 20,
    });

    const record = screen.getByTestId(`research-debate-item-${HASH_ONE}`);
    fireEvent.click(within(record).getByRole('button'));

    expect(await screen.findByText(/Value supports upside/)).toBeInTheDocument();
    expect(screen.getAllByText(/window.__debateXss=true/)).toHaveLength(3);
    expect(container.querySelector('img')).toBeNull();
    expect(container.querySelector('script')).toBeNull();
    expect(container.querySelector('svg[onload]')).toBeNull();
    expect(container.querySelector('[title]')).toBeNull();
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
    expect(screen.getByLabelText(HASH_ONE)).toBeInTheDocument();
    expect(screen.getByLabelText(BULL_TURN_HASH)).toBeInTheDocument();
    expect(screen.getAllByLabelText(PROMPT_FINGERPRINT)).toHaveLength(2);
  });

  it('ignores a stale response after the task changes', async () => {
    let resolveFirst: (value: Awaited<ReturnType<typeof researchApi.listDebates>>) => void = () => undefined;
    vi.mocked(researchApi.listDebates).mockImplementation(({ jobId }) => {
      if (jobId === 'task-1') {
        return new Promise((resolve) => {
          resolveFirst = resolve;
        });
      }
      return Promise.resolve({
        items: [summary(HASH_TWO, '000001')],
        count: 1,
        nextCursor: null,
      });
    });
    const { rerender } = render(<ResearchDebatePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究辩论/ }));
    await waitFor(() => expect(researchApi.listDebates).toHaveBeenCalledTimes(1));

    rerender(<ResearchDebatePanel taskId="task-2" />);
    expect(await screen.findByTestId(`research-debate-item-${HASH_TWO}`)).toBeInTheDocument();

    await act(async () => {
      resolveFirst({ items: [summary(HASH_ONE)], count: 1, nextCursor: null });
    });

    expect(screen.queryByTestId(`research-debate-item-${HASH_ONE}`)).not.toBeInTheDocument();
    expect(screen.getByTestId(`research-debate-item-${HASH_TWO}`)).toBeInTheDocument();
  });

  it('loads the next cursor page and de-duplicates immutable hashes', async () => {
    vi.mocked(researchApi.listDebates)
      .mockResolvedValueOnce({
        items: [summary(HASH_ONE)],
        count: 1,
        nextCursor: 'opaque-next',
      })
      .mockResolvedValueOnce({
        items: [summary(HASH_ONE), summary(HASH_TWO)],
        count: 2,
        nextCursor: null,
      });
    render(<ResearchDebatePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究辩论/ }));
    await screen.findByTestId(`research-debate-item-${HASH_ONE}`);

    fireEvent.click(screen.getByRole('button', { name: '加载更多' }));

    expect(await screen.findByTestId(`research-debate-item-${HASH_TWO}`)).toBeInTheDocument();
    expect(screen.getAllByTestId(/research-debate-item-/)).toHaveLength(2);
    expect(researchApi.listDebates).toHaveBeenNthCalledWith(2, {
      jobId: 'task-1',
      cursor: 'opaque-next',
      limit: 20,
    });
  });

  it('renders typed stance failures and retries a failed detail request', async () => {
    vi.mocked(researchApi.listDebates).mockResolvedValue({
      items: [summary(HASH_ONE)],
      count: 1,
      nextCursor: null,
    });
    const partialDetail = detail();
    partialDetail.status = 'partial';
    partialDetail.debate.status = 'partial';
    partialDetail.debate.failedStances = [{
      stance: 'bear',
      errorCode: 'provider_terminal',
    }];
    partialDetail.debate.turns = partialDetail.debate.turns.slice(0, 1);
    partialDetail.debate.bearTurnHash = null;
    partialDetail.bearTurnHash = null;
    partialDetail.bearArgumentCount = 0;
    vi.mocked(researchApi.getDebate)
      .mockRejectedValueOnce({ response: { status: 500, data: { message: 'temporary' } } })
      .mockResolvedValueOnce(partialDetail);
    render(<ResearchDebatePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究辩论/ }));
    const record = await screen.findByTestId(`research-debate-item-${HASH_ONE}`);

    fireEvent.click(within(record).getByRole('button'));
    expect(await within(record).findByText('辩论详情加载失败')).toBeInTheDocument();
    fireEvent.click(within(record).getByRole('button', { name: '重试' }));

    expect(await within(record).findByText('空方观点 (provider_terminal)')).toBeInTheDocument();
    expect(researchApi.getDebate).toHaveBeenCalledTimes(2);
  });

  it('shows a neutral empty state and retries a failed list request', async () => {
    vi.mocked(researchApi.listDebates)
      .mockRejectedValueOnce({ response: { status: 500, data: { message: 'temporary' } } })
      .mockResolvedValueOnce({ items: [], count: 0, nextCursor: null });
    render(<ResearchDebatePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究辩论/ }));

    expect(await screen.findByText('研究辩论加载失败')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '重试' }));

    expect(await screen.findByText('本次任务没有研究辩论')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
