import { Test, TestingModule } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { OpensearchConsumerMicroserviceModule } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.module';
import { OpensearchConsumerMicroserviceController } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';
import { OpenSearchService, WIKIMEDIA_INDEX } from '@app/opensearch';

describe('Opensearch consumer (functional)', () => {
  let controller: OpensearchConsumerMicroserviceController;
  let service: OpensearchConsumerMicroserviceService;
  let ensureIndex: jest.Mock;
  let bulkIndex: jest.Mock;

  beforeAll(async () => {
    ensureIndex = jest.fn().mockResolvedValue(undefined);
    bulkIndex = jest.fn().mockResolvedValue({ count: 0, errors: false });
    const module: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
        OpensearchConsumerMicroserviceModule,
      ],
    })
      .overrideProvider(OpenSearchService)
      .useValue({
        ensureIndex,
        bulkIndex,
      })
      .compile();

    controller = module.get(OpensearchConsumerMicroserviceController);
    service = module.get(OpensearchConsumerMicroserviceService);
    await service.onModuleInit();
  });

  beforeEach(() => {
    bulkIndex.mockClear();
  });

  it('should ensure index on module init', () => {
    expect(ensureIndex).toHaveBeenCalledWith(WIKIMEDIA_INDEX);
  });

  it('should enqueue message and should not call bulkIndex until flush (under batch size)', () => {
    controller.handleWikimediaRecentChange(
      JSON.stringify({
        payload: { meta: { id: 'fc-1' }, title: 'Functional doc' },
      }),
    );
    expect(bulkIndex).not.toHaveBeenCalled();
  });

  it('should bulk index documents on module destroy (flush buffer)', async () => {
    controller.handleWikimediaRecentChange(
      JSON.stringify({
        payload: { meta: { id: 'fc-flush-1' }, title: 'Flush doc' },
      }),
    );
    service.onModuleDestroy();
    await Promise.resolve();

    expect(bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.arrayContaining([
        expect.objectContaining({
          id: 'fc-flush-1',
          source: expect.objectContaining({ title: 'Flush doc' }),
        }),
      ]),
    );
  });

  it('should accept object payload and flush on destroy', async () => {
    controller.handleWikimediaRecentChange({
      payload: { meta: { id: 'fc-obj-1' }, title: 'Object payload' },
    });
    service.onModuleDestroy();
    await Promise.resolve();

    expect(bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.arrayContaining([
        expect.objectContaining({
          id: 'fc-obj-1',
          source: expect.anything(),
        }),
      ]),
    );
  });

  it('should not index message without payload.meta.id', async () => {
    controller.handleWikimediaRecentChange(
      JSON.stringify({ payload: { title: 'No id' } }),
    );
    service.onModuleDestroy();
    await Promise.resolve();

    expect(bulkIndex).not.toHaveBeenCalled();
  });
});
