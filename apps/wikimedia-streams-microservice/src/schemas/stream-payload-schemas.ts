import { TOPICS } from '@app/contracts';

/**
 * Avro record schema shape for Schema Registry register().
 */
export interface AvroRecordSchema {
  type: 'record';
  name: string;
  fields: Array<{
    name: string;
    type: string | [string, string];
    default?: null | number;
  }>;
}

/**
 * Avro schemas for the three stream topics (Confluent Schema Registry).
 * Subject naming: <topic>-value.
 */

const BOT_COUNT_VALUE_SCHEMA = {
  type: 'record',
  name: 'BotCountValue',
  fields: [
    { name: 'bot', type: ['null', 'long'], default: null },
    { name: 'non_bot', type: ['null', 'long'], default: null },
  ],
} as const;

const WEBSITE_COUNT_VALUE_SCHEMA = {
  type: 'record',
  name: 'WebsiteCountValue',
  fields: [
    { name: 'count', type: 'long' },
    { name: 'website', type: 'string' },
  ],
} as const;

const EVENTS_TS_VALUE_SCHEMA = {
  type: 'record',
  name: 'EventCountTimeseriesValue',
  fields: [
    { name: 'event_count', type: 'long' },
    { name: 'start_time', type: 'string' },
    { name: 'end_time', type: 'string' },
    { name: 'window_size', type: 'long' },
  ],
} as const;

export const STREAM_SCHEMAS = {
  [TOPICS.BOT_COUNT]: {
    subject: `${TOPICS.BOT_COUNT}-value`,
    schema: BOT_COUNT_VALUE_SCHEMA,
  },
  [TOPICS.WEBSITE_COUNT]: {
    subject: `${TOPICS.WEBSITE_COUNT}-value`,
    schema: WEBSITE_COUNT_VALUE_SCHEMA,
  },
  [TOPICS.EVENTS_TS]: {
    subject: `${TOPICS.EVENTS_TS}-value`,
    schema: EVENTS_TS_VALUE_SCHEMA,
  },
} as const;

export type StreamTopicWithSchema = keyof typeof STREAM_SCHEMAS;
