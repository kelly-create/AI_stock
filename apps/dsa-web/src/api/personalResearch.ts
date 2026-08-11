import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  PersonalResearchRunAccepted,
  PersonalResearchRunRequest,
} from '../types/personalResearch';

export const personalResearchApi = {
  async createRun(
    payload: PersonalResearchRunRequest,
    idempotencyKey: string,
  ): Promise<PersonalResearchRunAccepted> {
    const response = await apiClient.post<Record<string, unknown>>(
      '/api/v1/research/personal/runs',
      {
        stock_code: payload.stockCode,
        requested_mode: payload.requestedMode,
        notify: payload.notify,
        report_language: payload.reportLanguage,
      },
      {
        headers: { 'Idempotency-Key': idempotencyKey },
      },
    );
    return toCamelCase<PersonalResearchRunAccepted>(response.data);
  },
};
