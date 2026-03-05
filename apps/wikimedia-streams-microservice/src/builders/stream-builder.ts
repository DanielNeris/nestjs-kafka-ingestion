import type { EventEnvelope } from '@app/contracts';
import type {
  BotCountStreamBuilder,
  EventCountTimeseriesBuilder,
  StreamEmission,
  WebsiteCountStreamBuilder,
} from '../types/stream.types';

/**
 * Composes the three stream builders and returns all emissions for a single RecentChange event.
 * All three builders are stateful and must be the same instance across all events.
 */
export function buildStreamEmissions(
  envelope: EventEnvelope<Record<string, unknown>>,
  botCountBuilder: BotCountStreamBuilder,
  websiteCountBuilder: WebsiteCountStreamBuilder,
  eventCountTimeseriesBuilder: EventCountTimeseriesBuilder,
): StreamEmission[] {
  const bot = botCountBuilder.process(envelope);
  const website = websiteCountBuilder.process(envelope);
  const timeseries = eventCountTimeseriesBuilder.process(envelope);
  return [...bot, ...website, ...timeseries];
}
