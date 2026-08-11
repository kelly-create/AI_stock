import type { DecisionAction } from '../types/analysis';
import type {
  DecisionSignalAccountAction,
  DecisionSignalItem,
} from '../types/decisionSignals';

const ACCOUNT_ACTION_TO_DECISION_ACTION: Record<DecisionSignalAccountAction, DecisionAction> = {
  observe: 'watch',
  open_candidate: 'buy',
  add_candidate: 'add',
  hold: 'hold',
  reduce_candidate: 'reduce',
  exit_candidate: 'sell',
};

/** Resolve the Policy-adjusted action that controls execution-oriented views. */
export function getDecisionSignalPrimaryAction(item: DecisionSignalItem): DecisionAction {
  return item.accountAction
    ? ACCOUNT_ACTION_TO_DECISION_ACTION[item.accountAction]
    : item.action;
}
