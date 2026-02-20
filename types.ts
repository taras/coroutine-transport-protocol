import type { Effect } from "effection";

export interface WorkflowEffect extends Effect<unknown> {
  workflowId: string;
  effectId: string;
}

export interface Workflow<T> {
  [Symbol.iterator](): Iterator<WorkflowEffect, T, unknown>;
};
