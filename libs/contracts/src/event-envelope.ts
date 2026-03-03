/**
 * Standard event envelope for all events in the system.
 * This ensures consistency across microservices.
 */

import type { EventType, TopicName } from './topics';

export type EventEnvelope<TPayload> = {
  eventId: string; // UUID for idempotency
  type?: string; // Event name (e.g. "OrderCreated")
  version?: number; // Version of event schema
  occurredAt: string; // ISO date
  traceId?: string; // Optional tracing ID
  payload?: TPayload; // Actual event data
  headers?: Record<string, string>; // Optional headers
};

export type EmitOptions<TPayload> = {
  topic: TopicName;
  type?: EventType;
  payload?: TPayload;
  version?: number;
  key?: string;
  headers?: Record<string, string>;
};
