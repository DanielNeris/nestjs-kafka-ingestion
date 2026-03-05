import type { EventEnvelope } from '@app/contracts';
import { TOPICS } from '@app/contracts';
import { WINDOW_SIZE_MS } from '../constants/event-count-timeseries.constants';
import type {
  EventCountTimeseriesBuilder,
  EventCountTimeseriesPayload,
  StreamEmission,
} from '../types/stream.types';

/**
 * Creates a stateful builder that aggregates events into fixed time windows (10s).
 * Each emission has payload: { event_count, start_time, end_time, window_size }.
 */
export function createEventCountTimeseriesBuilder(): EventCountTimeseriesBuilder {
  const windowCounts = new Map<string, number>();

  return {
    process(
      envelope: EventEnvelope<Record<string, unknown>>,
    ): StreamEmission[] {
      const payload = envelope?.payload;
      if (!payload || typeof payload !== 'object') return [];

      const occurredAt = envelope.occurredAt ?? new Date().toISOString();
      const ts = new Date(occurredAt).getTime();
      const startTs = Math.floor(ts / WINDOW_SIZE_MS) * WINDOW_SIZE_MS;
      const endTs = startTs + WINDOW_SIZE_MS;
      const start_time = new Date(startTs).toISOString();
      const end_time = new Date(endTs).toISOString();

      const count = (windowCounts.get(start_time) ?? 0) + 1;
      windowCounts.set(start_time, count);

      const payloadOut: EventCountTimeseriesPayload = {
        event_count: count,
        start_time,
        end_time,
        window_size: WINDOW_SIZE_MS,
      };

      return [
        {
          topic: TOPICS.EVENTS_TS,
          key: start_time,
          payload: payloadOut,
        },
      ];
    },
  };
}
