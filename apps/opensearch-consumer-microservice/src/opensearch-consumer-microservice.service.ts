import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { OpenSearchService, WIKIMEDIA_INDEX } from '@app/opensearch';
import type { EventEnvelope } from '@app/contracts';

const BATCH_SIZE = 50;
const FLUSH_MS = 2000;

interface BufferedDoc {
  id: string;
  source: Record<string, unknown>;
}

@Injectable()
export class OpensearchConsumerMicroserviceService
  implements OnModuleInit, OnModuleDestroy
{
  private readonly logger = new Logger(
    OpensearchConsumerMicroserviceService.name,
  );
  private buffer: BufferedDoc[] = [];
  private flushTimer: ReturnType<typeof setInterval> | null = null;

  constructor(private readonly openSearch: OpenSearchService) {}

  async onModuleInit(): Promise<void> {
    await this.openSearch.ensureIndex(WIKIMEDIA_INDEX);
    this.logger.log(`Index "${WIKIMEDIA_INDEX}" ensured`);
    this.scheduleFlush();
  }

  onModuleDestroy(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    this.flush();
  }

  handleMessage(raw: string | Record<string, unknown>): void {
    try {
      const envelope = (
        typeof raw === 'string' ? JSON.parse(raw) : raw
      ) as EventEnvelope<Record<string, unknown>>;
      const payload = envelope?.payload;
      if (!payload || typeof payload !== 'object') return;

      // Idempotent indexing: use payload.meta.id as OpenSearch _id so re-deliveries overwrite the same doc.
      const meta = payload.meta as { id?: string } | undefined;
      const id = meta?.id;
      this.logger.log(`id: ${id}`);
      if (!id || typeof id !== 'string') return;

      // Dedupe by id in buffer (last write wins) to avoid version conflicts in bulk when same event appears twice.
      this.buffer = this.buffer.filter((b) => b.id !== id);
      this.buffer.push({ id, source: payload });
      if (this.buffer.length >= BATCH_SIZE) {
        this.flush();
      }
    } catch (e) {
      this.logger.warn(
        `Failed to parse/enqueue message: ${(e as Error).message}`,
      );
    }
  }

  private scheduleFlush(): void {
    if (this.flushTimer) return;
    this.flushTimer = setInterval(() => {
      this.flush();
    }, FLUSH_MS);
  }

  private flush(): void {
    if (this.buffer.length === 0) return;
    const batch = this.buffer.splice(0, this.buffer.length);
    // One doc per id (last wins) so bulk never has duplicate _id → fewer version_conflict errors.
    const byId = new Map<string, Record<string, unknown>>();
    for (const { id, source } of batch) byId.set(id, source);
    const items = Array.from(byId.entries(), ([id, source]) => ({
      id,
      source,
    }));
    this.openSearch
      .bulkIndex(WIKIMEDIA_INDEX, items)
      .then(({ count, errors }) => {
        this.logger.log(`Bulk indexed ${count} record(s). errors=${errors}`);
      })
      .catch((e) => {
        this.logger.error('Bulk index failed', e);
      });
  }
}
