import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import { lastValueFrom } from 'rxjs';
import { KAFKA_CLIENT } from './kafka.constants';
import { EventEnvelope, EmitOptions } from '@app/contracts';

@Injectable()
export class KafkaProducer implements OnModuleInit {
  constructor(@Inject(KAFKA_CLIENT) private readonly client: ClientKafka) {}

  async onModuleInit() {
    await this.client.connect();
  }

  /**
   * Emit an event with a standard envelope.
   * `key` is important for partitioning and per-entity ordering.
   */
  async emit<TPayload>({
    topic,
    type,
    payload,
    key,
    version = 1,
    headers,
  }: EmitOptions<TPayload>) {
    const envelope: EventEnvelope<TPayload> = {
      eventId: randomUUID(),
      type,
      version,
      traceId: headers?.['X-Trace-Id'],
      occurredAt: new Date().toISOString(),
      payload,
      headers,
    };

    // Nest Kafka transport supports kafkajs-like message format:
    // { key, value, headers }
    await lastValueFrom(
      this.client.emit(topic, {
        key,
        value: envelope,
        headers,
      }),
    );
  }

  /**
   * Emit a message with Schema Registry–encoded value (buffer). No envelope.
   * Use for topics that use Confluent serialization (Avro).
   */
  async emitEncoded(
    topic: string,
    key: string,
    valueBuffer: Buffer,
    headers?: Record<string, string>,
  ): Promise<void> {
    await lastValueFrom(
      this.client.emit(topic, {
        key,
        value: valueBuffer,
        headers,
      }),
    );
  }
}
