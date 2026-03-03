import { Test, TestingModule } from '@nestjs/testing';
import { WikimediaProducerMicroserviceController } from '@app/wikimedia-producer-microservice/wikimedia-producer-microservice.controller';
import { WikimediaProducerMicroserviceService } from '@app/wikimedia-producer-microservice/wikimedia-producer-microservice.service';

describe('WikimediaProducerMicroserviceController', () => {
  let controller: WikimediaProducerMicroserviceController;
  let onModuleInit: jest.Mock;

  beforeEach(async () => {
    onModuleInit = jest.fn();
    const module: TestingModule = await Test.createTestingModule({
      controllers: [WikimediaProducerMicroserviceController],
      providers: [
        {
          provide: WikimediaProducerMicroserviceService,
          useValue: { onModuleInit },
        },
      ],
    }).compile();

    controller = module.get(WikimediaProducerMicroserviceController);
  });

  it('should call service.onModuleInit on GET /', () => {
    controller.onModuleInit();
    expect(onModuleInit).toHaveBeenCalledTimes(1);
  });

  it('should use injected service when controller is defined', () => {
    expect(controller).toBeDefined();
    controller.onModuleInit();
    expect(onModuleInit).toHaveBeenCalled();
  });
});
