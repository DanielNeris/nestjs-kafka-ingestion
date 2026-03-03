import { Test, TestingModule } from '@nestjs/testing';
import { WikimediaProducerMicroserviceService } from '@app/wikimedia-producer-microservice/wikimedia-producer-microservice.service';
import { WikimediaStreamHandler } from '@app/wikimedia-producer-microservice/wikimedia-stream.handler';

describe('WikimediaProducerMicroserviceService', () => {
  let service: WikimediaProducerMicroserviceService;
  let streamHandler: { start: jest.Mock; stop: jest.Mock };

  beforeEach(async () => {
    streamHandler = { start: jest.fn(), stop: jest.fn() };
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WikimediaProducerMicroserviceService,
        {
          provide: WikimediaStreamHandler,
          useValue: streamHandler,
        },
      ],
    }).compile();

    service = module.get(WikimediaProducerMicroserviceService);
  });

  it('should call streamHandler.start on module init', () => {
    service.onModuleInit();
    expect(streamHandler.start).toHaveBeenCalledTimes(1);
  });

  it('should call streamHandler.stop on module destroy', () => {
    service.onModuleDestroy();
    expect(streamHandler.stop).toHaveBeenCalledTimes(1);
  });
});
