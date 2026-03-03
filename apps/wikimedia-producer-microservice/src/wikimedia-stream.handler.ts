import { EVENT_TYPES, TOPICS } from '@app/contracts';
import { KafkaProducer } from '@app/kafka';
import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { EventSource } from 'eventsource';

import {
  RECONNECT_DELAY_MS,
  WIKIMEDIA_STREAM_URL,
} from './constants/wikimedia-stream.constants';
import type { WikimediaRecentChangePayload } from './types/wikimedia-recent-change.types';

/**
 * Handles SSE lifecycle (onopen, onerror, onmessage) for the Wikimedia recentchange stream.
 * Produces each event to Kafka.
 */
@Injectable()
export class WikimediaStreamHandler {
  private readonly logger = new Logger(WikimediaStreamHandler.name);
  private es: EventSource | undefined;

  constructor(private readonly kafkaProducer: KafkaProducer) {}

  start(): void {
    this.logger.log(`Connecting to SSE: ${WIKIMEDIA_STREAM_URL}`);
    this.es = new EventSource(WIKIMEDIA_STREAM_URL);

    this.es.onopen = (): void => this.onopen();
    this.es.onerror = (e: Event): void => this.onerror(e);
    this.es.onmessage = (e: MessageEvent): void => {
      void this.onmessage(e);
    };
  }

  stop(): void {
    this.es?.close();
    this.es = undefined;
  }

  private onopen(): void {
    this.logger.log('SSE connection opened ✅');
  }

  private onerror(err: Event): void {
    this.logger.error('SSE error ❌', err);

    this.es?.close();
    this.es = undefined;
    setTimeout(() => this.start(), RECONNECT_DELAY_MS);
  }

  private async onmessage(msg: MessageEvent): Promise<void> {
    try {
      const payload = JSON.parse(
        msg.data as string,
      ) as WikimediaRecentChangePayload;
      // Key: meta.id is unique per event → spreads messages across partitions.
      // Use payload.wiki first if you prefer ordering by wiki (same wiki in same partition).
      const key =
        payload.meta?.id ??
        payload.wiki ??
        payload.title ??
        payload.user ??
        randomUUID();

      await this.kafkaProducer.emit({
        topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
        type: EVENT_TYPES.WIKIMEDIA_RECENTCHANGES,
        key: String(key),
        payload,
        headers: {
          'X-Trace-Id': randomUUID(),
          source: 'wikimedia-sse',
        },
        version: 1,
      });
    } catch (e) {
      this.logger.warn(
        `Failed to parse/emit SSE message: ${(e as Error).message}`,
      );
    }
  }
}
