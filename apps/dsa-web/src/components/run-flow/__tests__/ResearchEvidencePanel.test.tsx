import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { researchApi } from '../../../api/research';
import type {
  ResearchEvidenceDetailResponse,
  ResearchEvidenceSummary,
} from '../../../types/research';
import { ResearchEvidencePanel } from '../ResearchEvidencePanel';

vi.mock('../../../api/research', () => ({
  researchApi: {
    listEvidence: vi.fn(),
    getEvidence: vi.fn(),
  },
}));

const HASH_ONE = 'a'.repeat(64);
const HASH_TWO = 'b'.repeat(64);
const FACTOR_HASH = 'c'.repeat(64);

const summary = (
  evidenceHash: string,
  stockCode = '600519',
): ResearchEvidenceSummary => ({
  id: evidenceHash === HASH_ONE ? 1 : 2,
  stockCode,
  market: 'cn',
  evidenceEngineVersion: 'evidence-v1',
  claimPolicyVersion: 'claim-policy-v1',
  asOf: '2026-08-08T08:00:00Z',
  availableAt: '2026-08-08T08:00:00Z',
  status: 'partial',
  coverage: 0.5,
  claimCount: 1,
  citationCount: 2,
  inputDatasetHashes: [FACTOR_HASH],
  factorSnapshotHash: FACTOR_HASH,
  evidenceHash,
  originJobId: 'task-1',
  createdAt: '2026-08-08T08:00:00Z',
});

const detail = (evidenceHash = HASH_ONE): ResearchEvidenceDetailResponse => ({
  ...summary(evidenceHash),
  evidence: {
    evidenceEngineVersion: 'evidence-v1',
    claimPolicyVersion: 'claim-policy-v1',
    stockCode: '600519',
    market: 'cn',
    asOf: '2026-08-08T08:00:00Z',
    availableAt: '2026-08-08T08:00:00Z',
    status: 'partial',
    coverage: 0.5,
    inputDatasetHashes: [FACTOR_HASH],
    factorSnapshotHash: FACTOR_HASH,
    limitations: ['Only one reporting period is available.'],
    claims: [{
      id: 'claim-1',
      kind: 'factor_metric',
      statement: '<img src=x onerror="window.__evidenceXss=true"> Value is supported.',
      status: 'supported',
      citationIds: ['citation-safe', 'citation-unsafe'],
      limitations: [],
      availableAt: '2026-08-08T08:00:00Z',
    }],
    citations: [
      {
        id: 'citation-safe',
        relation: 'supports',
        artifactType: 'factor',
        artifactHash: FACTOR_HASH,
        jsonPointer: '/factors/value/score',
        valueHash: 'd'.repeat(64),
        availableAt: '2026-08-08T08:00:00Z',
        sourceName: 'factor_snapshot',
        title: 'Value score',
        excerpt: '72',
        canonicalUrl: 'https://example.com/value',
      },
      {
        id: 'citation-unsafe',
        relation: 'context',
        artifactType: 'dataset',
        artifactHash: FACTOR_HASH,
        jsonPointer: '/rows/0',
        valueHash: 'e'.repeat(64),
        availableAt: '2026-08-08T08:00:00Z',
        sourceName: 'dataset_snapshot',
        title: 'Unsafe source',
        excerpt: 'Rendered as text only.',
        canonicalUrl: 'javascript:alert(1)',
      },
    ],
  },
});

describe('ResearchEvidencePanel', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('loads lazily, shows summary metadata, and renders claims and safe links as text', async () => {
    vi.mocked(researchApi.listEvidence).mockResolvedValue({
      items: [summary(HASH_ONE)],
      count: 1,
      nextCursor: null,
    });
    vi.mocked(researchApi.getEvidence).mockResolvedValue(detail());
    const { container } = render(<ResearchEvidencePanel taskId="task-1" />);

    expect(researchApi.listEvidence).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button', { name: /研究证据/ }));

    expect(await screen.findByText('覆盖率 50%')).toBeInTheDocument();
    expect(screen.getByText('1 条主张 / 2 条引用')).toBeInTheDocument();
    expect(researchApi.listEvidence).toHaveBeenCalledWith({
      jobId: 'task-1',
      limit: 20,
    });

    const record = screen.getByTestId(`research-evidence-item-${HASH_ONE}`);
    fireEvent.click(within(record).getByRole('button'));

    expect(await screen.findByText(/Value is supported/)).toBeInTheDocument();
    expect(container.querySelector('img')).toBeNull();
    const sourceLink = screen.getByRole('link', { name: /打开来源/ });
    expect(sourceLink).toHaveAttribute('href', 'https://example.com/value');
    expect(sourceLink).toHaveAttribute('target', '_blank');
    expect(sourceLink).toHaveAttribute('rel', 'noopener noreferrer');
    expect(screen.getAllByRole('link')).toHaveLength(1);
    expect(container.querySelector('[title]')).toBeNull();
    expect(screen.getByLabelText(HASH_ONE)).toBeInTheDocument();
    expect(screen.getAllByLabelText(FACTOR_HASH)).toHaveLength(2);
  });

  it('ignores a stale response after the task changes', async () => {
    let resolveFirst: (value: Awaited<ReturnType<typeof researchApi.listEvidence>>) => void = () => undefined;
    vi.mocked(researchApi.listEvidence).mockImplementation(({ jobId }) => {
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
    const { rerender } = render(<ResearchEvidencePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究证据/ }));
    await waitFor(() => expect(researchApi.listEvidence).toHaveBeenCalledTimes(1));

    rerender(<ResearchEvidencePanel taskId="task-2" />);
    expect(await screen.findByTestId(`research-evidence-item-${HASH_TWO}`)).toBeInTheDocument();

    await act(async () => {
      resolveFirst({ items: [summary(HASH_ONE)], count: 1, nextCursor: null });
    });

    expect(screen.queryByTestId(`research-evidence-item-${HASH_ONE}`)).not.toBeInTheDocument();
    expect(screen.getByTestId(`research-evidence-item-${HASH_TWO}`)).toBeInTheDocument();
  });

  it('loads the next cursor page and de-duplicates immutable hashes', async () => {
    vi.mocked(researchApi.listEvidence)
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
    render(<ResearchEvidencePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究证据/ }));
    await screen.findByTestId(`research-evidence-item-${HASH_ONE}`);

    fireEvent.click(screen.getByRole('button', { name: '加载更多' }));

    expect(await screen.findByTestId(`research-evidence-item-${HASH_TWO}`)).toBeInTheDocument();
    expect(screen.getAllByTestId(/research-evidence-item-/)).toHaveLength(2);
    expect(researchApi.listEvidence).toHaveBeenNthCalledWith(2, {
      jobId: 'task-1',
      cursor: 'opaque-next',
      limit: 20,
    });
  });

  it('shows neutral empty state and retries a failed list request', async () => {
    vi.mocked(researchApi.listEvidence)
      .mockRejectedValueOnce({ response: { status: 500, data: { message: 'temporary' } } })
      .mockResolvedValueOnce({ items: [], count: 0, nextCursor: null });
    render(<ResearchEvidencePanel taskId="task-1" />);
    fireEvent.click(screen.getByRole('button', { name: /研究证据/ }));

    expect(await screen.findByText('研究证据加载失败')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: '重试' }));

    expect(await screen.findByText('本次任务没有研究证据')).toBeInTheDocument();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });
});
