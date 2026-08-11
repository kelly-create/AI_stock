export type PersonalResearchMode = 'auto' | 'quick' | 'standard' | 'deep' | 'debate';
export type ResolvedPersonalResearchMode = Exclude<PersonalResearchMode, 'auto'>;

export interface PersonalResearchRunRequest {
  stockCode: string;
  requestedMode: PersonalResearchMode;
  notify: boolean;
  reportLanguage?: string;
}

export interface PersonalResearchRunAccepted {
  taskId: string;
  traceId: string;
  status: 'pending' | 'processing';
  created: boolean;
  deduplicated: boolean;
  stockCode: string;
  market: 'cn';
  requestedMode: PersonalResearchMode;
  resolvedMode: ResolvedPersonalResearchMode;
  priority: number;
}

export interface PersonalResearchResultSummary {
  contractVersion: 'personal-research-artifacts-v1';
  researchSnapshotHash: string;
  skillExecutionHashes: Record<string, string>;
  debateSnapshotHash: string | null;
  debateReviewHash: string | null;
  thesisHash: string | null;
  decisionSignal: Record<string, unknown> | null;
}
