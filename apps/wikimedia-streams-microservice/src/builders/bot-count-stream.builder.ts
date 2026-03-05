import type { EventEnvelope } from '@app/contracts';
import { TOPICS } from '@app/contracts';
import type {
  BotCountStreamBuilder,
  StreamEmission,
} from '../types/stream.types';

/**
 * Creates a stateful builder that keeps running counts of bot vs non-bot.
 * Each call to process() updates the counts and returns two emissions:
 * - key "bot"   -> payload { bot: number }
 * - key "non-bot" -> payload { "non-bot": number }
 * So the topic always has two logical records that get updated over time.
 */
export function createBotCountStreamBuilder(): BotCountStreamBuilder {
  let botCount = 0;
  let nonBotCount = 0;

  return {
    process(
      envelope: EventEnvelope<Record<string, unknown>>,
    ): StreamEmission[] {
      const payload = envelope?.payload;
      if (!payload || typeof payload !== 'object') return [];

      const isBot = payload.bot === true;
      if (isBot) botCount += 1;
      else nonBotCount += 1;

      return [
        {
          topic: TOPICS.BOT_COUNT,
          key: 'bot',
          payload: { bot: botCount },
        },
        {
          topic: TOPICS.BOT_COUNT,
          key: 'non-bot',
          payload: { 'non-bot': nonBotCount },
        },
      ];
    },
  };
}
