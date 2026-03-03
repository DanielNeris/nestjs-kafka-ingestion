import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { of } from 'rxjs';
import { WikimediaProducerMicroserviceModule } from '@app/wikimedia-producer-microservice/wikimedia-producer-microservice.module';
import { WikimediaStreamHandler } from '@app/wikimedia-producer-microservice/wikimedia-stream.handler';
import { KAFKA_CLIENT } from '@app/kafka';

describe('WikimediaProducerMicroservice (e2e)', () => {
  let app: INestApplication;
  const streamHandler = { start: jest.fn(), stop: jest.fn() };

  beforeAll(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [WikimediaProducerMicroserviceModule],
    })
      .overrideProvider(WikimediaStreamHandler)
      .useValue(streamHandler)
      .overrideProvider(KAFKA_CLIENT)
      .useValue({
        connect: jest.fn().mockResolvedValue(undefined),
        emit: jest.fn().mockReturnValue(of(undefined)),
      })
      .compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  }, 10_000);

  afterAll(async () => {
    await app.close();
  });

  it('GET / triggers stream start (onModuleInit)', async () => {
    streamHandler.start.mockClear();
    const httpServer = app.getHttpServer() as Parameters<typeof request>[0];
    const res = await request(httpServer).get('/');
    expect(res.status).toBe(200);
    expect(streamHandler.start).toHaveBeenCalledTimes(1);
  });
});
