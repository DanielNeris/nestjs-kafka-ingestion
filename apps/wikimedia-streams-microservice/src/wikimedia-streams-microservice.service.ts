import { Injectable, Logger } from '@nestjs/common';
import type { EventEnvelope } from '@app/contracts';
import { KafkaProducer, SchemaRegistryService } from '@app/kafka';
import { TOPICS } from '@app/contracts';
import { createBotCountStreamBuilder } from './builders/bot-count-stream.builder';
import { createEventCountTimeseriesBuilder } from './builders/event-count-timeseries.builder';
import { createWebsiteCountStreamBuilder } from './builders/website-count-stream.builder';
import { buildStreamEmissions } from './builders/stream-builder';
import {
  type AvroRecordSchema,
  STREAM_SCHEMAS,
  type StreamTopicWithSchema,
} from './schemas/stream-payload-schemas';
import type { StreamEmission } from './types/stream.types';

@Injectable()
export class WikimediaStreamsMicroserviceService {
  private readonly logger = new Logger(
    WikimediaStreamsMicroserviceService.name,
  );
  private readonly botCountBuilder = createBotCountStreamBuilder();
  private readonly websiteCountBuilder = createWebsiteCountStreamBuilder();
  private readonly eventCountTimeseriesBuilder =
    createEventCountTimeseriesBuilder();
  private schemaIds = new Map<string, number>();

  constructor(
    private readonly kafkaProducer: KafkaProducer,
    private readonly schemaRegistry: SchemaRegistryService,
  ) {}

  /**
   * Process one RecentChange event: run stream builders and emit to bot/website/timeseries topics.
   * Payloads for the three stream topics are encoded with Confluent Schema Registry (Avro).
   */
  async handleRecentChange(
    raw: string | Record<string, unknown>,
  ): Promise<void> {
    try {
      const envelope = (
        typeof raw === 'string' ? JSON.parse(raw) : raw
      ) as EventEnvelope<Record<string, unknown>>;
      const emissions = buildStreamEmissions(
        envelope,
        this.botCountBuilder,
        this.websiteCountBuilder,
        this.eventCountTimeseriesBuilder,
      );
      for (const e of emissions) {
        await this.emitEncoded(e);
      }
    } catch (err) {
      this.logger.warn(`Stream processing failed: ${(err as Error).message}`);
    }
  }

  private async getSchemaId(topic: StreamTopicWithSchema): Promise<number> {
    const id = this.schemaIds.get(topic);
    if (id != null) return id;
    const { subject, schema } = STREAM_SCHEMAS[topic];
    const registry = this.schemaRegistry.getRegistry();
    const schemaCopy: AvroRecordSchema = JSON.parse(
      JSON.stringify(schema),
    ) as AvroRecordSchema;
    const { id: registeredId } = await registry.register(schemaCopy, {
      subject,
    });
    this.schemaIds.set(topic, registeredId);
    return registeredId;
  }

  /** Avro shape: both fields (non_bot — Avro does not allow hyphen in field names). */
  private normalizeBotPayload(payload: Record<string, unknown>): {
    bot: number | null;
    non_bot: number | null;
  } {
    return {
      bot: typeof payload.bot === 'number' ? payload.bot : null,
      non_bot:
        typeof payload['non-bot'] === 'number' ? payload['non-bot'] : null,
    };
  }

  private async emitEncoded(e: StreamEmission): Promise<void> {
    const topic = e.topic as StreamTopicWithSchema;
    const config = STREAM_SCHEMAS[topic];
    if (!config) return;
    const registry = this.schemaRegistry.getRegistry();
    const schemaId = await this.getSchemaId(topic);
    const payload =
      topic === TOPICS.BOT_COUNT
        ? this.normalizeBotPayload(e.payload as Record<string, unknown>)
        : (e.payload as Record<string, unknown>);
    const buffer = await registry.encode(schemaId, payload);
    await this.kafkaProducer.emitEncoded(topic, e.key, buffer);
  }
}
