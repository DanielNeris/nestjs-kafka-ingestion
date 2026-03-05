import type { EventEnvelope } from '@app/contracts';
import { TOPICS } from '@app/contracts';
import type {
  StreamEmission,
  WebsiteCountPayload,
  WebsiteCountStreamBuilder,
} from '../types/stream.types';

/**
 * Creates a stateful builder that keeps a running count per website.
 * Each emission has payload: { count, website }.
 */
export function createWebsiteCountStreamBuilder(): WebsiteCountStreamBuilder {
  const counts = new Map<string, number>();

  return {
    process(
      envelope: EventEnvelope<Record<string, unknown>>,
    ): StreamEmission[] {
      const payload = envelope?.payload;
      if (!payload || typeof payload !== 'object') return [];

      const website =
        (payload.wiki as string | undefined) ??
        (payload.server_name as string | undefined) ??
        'unknown';

      const count = (counts.get(website) ?? 0) + 1;
      counts.set(website, count);

      const payloadOut: WebsiteCountPayload = { count, website };

      return [
        {
          topic: TOPICS.WEBSITE_COUNT,
          key: website,
          payload: payloadOut,
        },
      ];
    },
  };
}
