/**
 * Central place for Kafka topic names.
 */

export const TOPICS = {
  WIKIMEDIA_RECENTCHANGES: 'wikimedia.recentchange',
  DLQ: 'dead-letter.queue',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];

export const EVENT_TYPES = {
  WIKIMEDIA_RECENTCHANGES: 'RecentChange',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];
