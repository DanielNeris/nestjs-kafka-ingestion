import { Test, TestingModule } from '@nestjs/testing';
import { OpensearchConsumerMicroserviceController } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.controller';
import { OpensearchConsumerMicroserviceService } from '@app/opensearch-consumer-microservice/opensearch-consumer-microservice.service';

describe('OpensearchConsumerMicroserviceController', () => {
  let controller: OpensearchConsumerMicroserviceController;
  let handleMessage: jest.Mock;

  beforeEach(async () => {
    handleMessage = jest.fn();
    const module: TestingModule = await Test.createTestingModule({
      controllers: [OpensearchConsumerMicroserviceController],
      providers: [
        {
          provide: OpensearchConsumerMicroserviceService,
          useValue: { handleMessage },
        },
      ],
    }).compile();

    controller = module.get(OpensearchConsumerMicroserviceController);
  });

  it('should delegate event payload (string) to service.handleMessage', () => {
    const raw = JSON.stringify({
      payload: { meta: { id: 'test-id' }, title: 'Foo' },
    });
    controller.handleWikimediaRecentChange(raw);
    expect(handleMessage).toHaveBeenCalledWith(raw);
  });

  it('should delegate event payload (object) to service.handleMessage', () => {
    const payload = { payload: { meta: { id: 'obj-id' }, title: 'Bar' } };
    controller.handleWikimediaRecentChange(payload);
    expect(handleMessage).toHaveBeenCalledWith(payload);
  });
});
