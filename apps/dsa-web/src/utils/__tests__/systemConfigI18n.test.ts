import { describe, expect, it } from 'vitest';
import { getCategoryDescription, getCategoryTitle } from '../systemConfigI18n';

describe('system config category localization', () => {
  it('renders the research category in Chinese and English', () => {
    expect(getCategoryTitle('research', undefined, 'zh')).toBe('个人投研');
    expect(getCategoryDescription('research', undefined, 'zh')).toBe(
      '管理个人 A 股投研能力的分阶段启用与策略门控。',
    );
    expect(getCategoryTitle('research', undefined, 'en')).toBe('Personal research');
    expect(getCategoryDescription('research', undefined, 'en')).toBe(
      'Manage staged rollout and policy gates for the personal A-share research system.',
    );
  });
});
