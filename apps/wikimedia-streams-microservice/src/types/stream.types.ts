import type { EventEnvelope } from '@app/contracts';

/**
 * Output record for the stream pipeline: topic + key + payload to emit.
 */
export interface StreamEmission<TPayload = unknown> {
  topic: string;
  key: string;
  payload: TPayload;
}

export interface BotCountStreamBuilder {
  process(envelope: EventEnvelope<Record<string, unknown>>): StreamEmission[];
}

export interface WebsiteCountPayload {
  count: number;
  website: string;
}

export interface WebsiteCountStreamBuilder {
  process(envelope: EventEnvelope<Record<string, unknown>>): StreamEmission[];
}

export interface EventCountTimeseriesPayload {
  event_count: number;
  start_time: string;
  end_time: string;
  window_size: number;
}

export interface EventCountTimeseriesBuilder {
  process(envelope: EventEnvelope<Record<string, unknown>>): StreamEmission[];
}
