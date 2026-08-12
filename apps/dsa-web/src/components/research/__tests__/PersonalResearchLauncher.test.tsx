import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { analysisApi } from '../../../api/analysis';
import { personalResearchApi } from '../../../api/personalResearch';
import { UiLanguageProvider } from '../../../contexts/UiLanguageContext';
import { UI_LANGUAGE_STORAGE_KEY } from '../../../utils/uiLanguage';
import { PersonalResearchLauncher } from '../PersonalResearchLauncher';

const { getLatestPersonalResearchThesis } = vi.hoisted(() => ({
  getLatestPersonalResearchThesis: vi.fn(),
}));

vi.mock('../../../api/personalResearch', () => ({
  personalResearchApi: { createRun: vi.fn() },
}));

vi.mock('../../../api/analysis', () => ({
  analysisApi: { getStatus: vi.fn() },
}));

vi.mock('../../../api/research', () => ({
  researchApi: { getLatestPersonalResearchThesis },
}));

function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}{location.search}</div>;
}

function renderLauncher(onTaskAccepted = vi.fn()) {
  window.localStorage.setItem(UI_LANGUAGE_STORAGE_KEY, 'en');
  render(
    <UiLanguageProvider>
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="*" element={(
            <>
              <PersonalResearchLauncher
                stockCode="600519"
                notify
                reportLanguage="en"
                onTaskAccepted={onTaskAccepted}
              />
              <LocationProbe />
            </>
          )} />
        </Routes>
      </MemoryRouter>
    </UiLanguageProvider>,
  );
  return onTaskAccepted;
}

describe('PersonalResearchLauncher', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.localStorage.clear();
    window.sessionStorage.clear();
    vi.mocked(personalResearchApi.createRun).mockResolvedValue({
      taskId: 'task-1',
      traceId: 'trace-1',
      status: 'pending',
      created: true,
      deduplicated: false,
      stockCode: '600519',
      market: 'cn',
      requestedMode: 'deep',
      resolvedMode: 'deep',
      priority: 50,
    });
    vi.mocked(analysisApi.getStatus).mockResolvedValue({
      taskId: 'task-1',
      traceId: 'trace-1',
      status: 'completed',
      stage: 'personal_research',
      progress: 100,
      result: {
        queryId: 'task-1',
        stockCode: '600519',
        stockName: 'Kweichow Moutai',
        createdAt: '2026-08-11T12:00:00',
        personalResearch: {
          contractVersion: 'personal-research-artifacts-v1',
          researchSnapshotHash: 'a'.repeat(64),
          skillExecutionHashes: {},
          debateSnapshotHash: null,
          debateReviewHash: null,
          thesisHash: 'b'.repeat(64),
          decisionSignal: { id: 42 },
        },
        report: {
          meta: {
            id: 31,
            queryId: 'task-1',
            stockCode: '600519',
            stockName: 'Kweichow Moutai',
            reportType: 'full',
            createdAt: '2026-08-11T12:00:00',
          },
          summary: {
            analysisSummary: 'summary',
            operationAdvice: 'observe',
            trendPrediction: 'neutral',
            sentimentScore: 50,
          },
        },
      },
    });
  });

  it('submits deep research, tracks completion, and opens the exact thesis signal', async () => {
    const onTaskAccepted = renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: 'Personal research' }));
    fireEvent.click(screen.getByRole('radio', { name: /Deep/ }));
    fireEvent.click(screen.getByRole('button', { name: 'Submit formal research' }));

    await waitFor(() => expect(personalResearchApi.createRun).toHaveBeenCalledWith(
      expect.objectContaining({ stockCode: '600519', requestedMode: 'deep', notify: true }),
      expect.stringMatching(/^web-personal-/),
    ));
    expect(onTaskAccepted).toHaveBeenCalledTimes(1);
    expect(await screen.findByText('Personal research completed')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Open thesis and decision signal' }));
    expect(screen.getByTestId('location')).toHaveTextContent('/decision-signals?signalId=42');
  });

  it('keeps the same idempotency key when a lost response is retried', async () => {
    vi.mocked(personalResearchApi.createRun)
      .mockRejectedValueOnce(new Error('connection lost'))
      .mockResolvedValueOnce({
        taskId: 'task-1',
        traceId: 'trace-1',
        status: 'pending',
        created: false,
        deduplicated: true,
        stockCode: '600519',
        market: 'cn',
        requestedMode: 'auto',
        resolvedMode: 'standard',
        priority: 50,
      });
    renderLauncher();
    fireEvent.click(screen.getByRole('button', { name: 'Personal research' }));
    fireEvent.click(screen.getByRole('button', { name: 'Submit formal research' }));
    await waitFor(() => expect(personalResearchApi.createRun).toHaveBeenCalledTimes(1));
    fireEvent.click(await screen.findByRole('button', { name: 'Submit formal research' }));
    await waitFor(() => expect(personalResearchApi.createRun).toHaveBeenCalledTimes(2));

    expect(vi.mocked(personalResearchApi.createRun).mock.calls[1][1])
      .toBe(vi.mocked(personalResearchApi.createRun).mock.calls[0][1]);
  });
});
