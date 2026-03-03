import { Test, TestingModule } from '@nestjs/testing';
import { OpenSearchService, WIKIMEDIA_INDEX } from '@app/opensearch';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';

describe('OpensearchConsumerMicroserviceService', () => {
  let service: OpensearchConsumerMicroserviceService;
  let openSearch: {
    ensureIndex: jest.Mock;
    bulkIndex: jest.Mock;
  };

  beforeEach(async () => {
    openSearch = {
      ensureIndex: jest.fn().mockResolvedValue(undefined),
      bulkIndex: jest.fn().mockResolvedValue({ count: 0, errors: false }),
    };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OpensearchConsumerMicroserviceService,
        { provide: OpenSearchService, useValue: openSearch },
      ],
    }).compile();

    service = module.get(OpensearchConsumerMicroserviceService);
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('should ensure index and schedule flush on module init', async () => {
    await service.onModuleInit();
    expect(openSearch.ensureIndex).toHaveBeenCalledWith(WIKIMEDIA_INDEX);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
    // Add one message so the timer flush has something to send
    service.handleMessage(
      JSON.stringify({
        payload: { meta: { id: 'timer-flush-id' }, title: 'Doc' },
      }),
    );
    jest.advanceTimersByTime(2000);
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.any(Array),
    );
  });

  it('should clear timer and flush buffer on module destroy', async () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    service.handleMessage(
      JSON.stringify({ payload: { meta: { id }, title: 'Doc' } }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.arrayContaining([expect.objectContaining({ id })]),
    );
  });

  it('should clear timer and flush when timer was set and module is destroyed', async () => {
    await service.onModuleInit();
    const id = 'timer-clear-id';
    service.handleMessage(
      JSON.stringify({ payload: { meta: { id }, title: 'Doc' } }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.any(Array),
    );
  });

  it('should not add second timer when scheduleFlush is already scheduled', async () => {
    await service.onModuleInit();
    const clearIntervalSpy = jest.spyOn(global, 'clearInterval');
    await service.onModuleInit();
    clearIntervalSpy.mockRestore();
    service.handleMessage(
      JSON.stringify({
        payload: { meta: { id: 'one' }, title: 'Doc' },
      }),
    );
    jest.advanceTimersByTime(2000);
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledTimes(1);
  });

  it('should log bulk result on flush success', async () => {
    await service.onModuleInit();
    service.handleMessage(
      JSON.stringify({
        payload: { meta: { id: 'log-id' }, title: 'Doc' },
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalled();
  });

  it('should enqueue valid envelope (string) and should not flush when under batch size', () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    const raw = JSON.stringify({
      payload: { meta: { id }, title: 'Doc title' },
    });
    service.handleMessage(raw);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should accept object input in handleMessage', () => {
    const id = 'uuid-' + Math.random().toString(36).slice(2);
    service.handleMessage({
      payload: { meta: { id }, title: 'Obj' },
    });
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should ignore message without payload', () => {
    service.handleMessage(JSON.stringify({}));
    service.handleMessage(JSON.stringify({ payload: null }));
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should ignore message when payload is not object', () => {
    service.handleMessage(JSON.stringify({ payload: 'string' }));
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should ignore message without payload.meta.id', () => {
    service.handleMessage(JSON.stringify({ payload: { title: 'No id' } }));
    service.handleMessage(JSON.stringify({ payload: { meta: {} } }));
    service.handleMessage(JSON.stringify({ payload: { meta: { id: 123 } } }));
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should flush when buffer reaches batch size (50)', async () => {
    await service.onModuleInit();
    for (let i = 0; i < 50; i++) {
      service.handleMessage(
        JSON.stringify({
          payload: { meta: { id: `id-${i}` }, title: `Doc ${i}` },
        }),
      );
    }
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.any(Array),
    );
    const firstCallArgs = openSearch.bulkIndex.mock.calls[0];
    expect(firstCallArgs?.[1]).toHaveLength(50);
  });

  it('should dedupe by id (last write wins)', async () => {
    await service.onModuleInit();
    const id = 'dedupe-key';
    for (let i = 0; i < 3; i++) {
      service.handleMessage(
        JSON.stringify({
          payload: { meta: { id }, title: `Doc v${i}` },
        }),
      );
    }
    for (let i = 0; i < 47; i++) {
      service.handleMessage(
        JSON.stringify({
          payload: { meta: { id: `id-${i}` }, title: `Doc ${i}` },
        }),
      );
    }
    // Buffer has 48 unique docs (3 same id → 1); trigger scheduled flush
    jest.advanceTimersByTime(2000);
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.any(Array),
    );
    type BufferedItem = { id: string; source: Record<string, unknown> };
    const firstCallArgs = openSearch.bulkIndex.mock.calls[0];
    const items = firstCallArgs?.[1] as BufferedItem[] | undefined;
    expect(items).toHaveLength(48);
    const byId = items?.find((x) => x.id === id);
    expect(byId).toBeDefined();
    expect(byId?.source.title).toBe('Doc v2');
  });

  it('should not throw on handleMessage parse error', () => {
    service.handleMessage('not json');
    service.handleMessage(undefined as unknown as string);
    expect(openSearch.bulkIndex).not.toHaveBeenCalled();
  });

  it('should log error when bulkIndex rejects', async () => {
    openSearch.bulkIndex.mockRejectedValueOnce(new Error('Bulk failed'));
    await service.onModuleInit();
    const id = 'err-id';
    service.handleMessage(
      JSON.stringify({ payload: { meta: { id }, title: 'One' } }),
    );
    service.onModuleDestroy();
    await Promise.resolve();
    expect(openSearch.bulkIndex).toHaveBeenCalled();
  });
});
