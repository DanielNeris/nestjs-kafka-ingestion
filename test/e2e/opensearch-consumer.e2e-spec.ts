import { Test, TestingModule } from '@nestjs/testing';
import { OpensearchConsumerMicroserviceModule } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.module';
import { OpensearchConsumerMicroserviceController } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';
import { OpenSearchService, WIKIMEDIA_INDEX } from '@app/opensearch';

describe('OpensearchConsumerMicroservice (e2e)', () => {
  let controller: OpensearchConsumerMicroserviceController;
  let service: OpensearchConsumerMicroserviceService;
  let bulkIndex: jest.Mock;

  beforeAll(async () => {
    bulkIndex = jest.fn().mockResolvedValue({ count: 0, errors: false });
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [OpensearchConsumerMicroserviceModule],
    })
      .overrideProvider(OpenSearchService)
      .useValue({
        ensureIndex: jest.fn().mockResolvedValue(undefined),
        bulkIndex,
      })
      .compile();

    controller = moduleFixture.get(OpensearchConsumerMicroserviceController);
    service = moduleFixture.get(OpensearchConsumerMicroserviceService);
    await service.onModuleInit();
  });

  it('handleWikimediaRecentChange enqueues message and flush on destroy sends to OpenSearch', async () => {
    const payload = JSON.stringify({
      payload: { meta: { id: 'e2e-id-1' }, title: 'E2E Doc' },
    });
    controller.handleWikimediaRecentChange(payload);
    expect(bulkIndex).not.toHaveBeenCalled();
    service.onModuleDestroy();
    await Promise.resolve();
    expect(bulkIndex).toHaveBeenCalledWith(
      WIKIMEDIA_INDEX,
      expect.arrayContaining([
        expect.objectContaining({
          id: 'e2e-id-1',
          source: expect.anything(),
        }),
      ]),
    );
  });
});
