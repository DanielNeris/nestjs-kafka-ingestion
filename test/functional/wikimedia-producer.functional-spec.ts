import { Test, TestingModule } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { EVENT_TYPES, TOPICS } from '@app/contracts';
import { WikimediaProducerMicroserviceModule } from '@app/wikimedia-producer-microservice/wikimedia-producer-microservice.module';
import { WikimediaStreamHandler } from '@app/wikimedia-producer-microservice/wikimedia-stream.handler';
import { KafkaProducer, KAFKA_CLIENT } from '@app/kafka';
import { of } from 'rxjs';
import { WIKIMEDIA_STREAM_URL } from '@app/wikimedia-producer-microservice/constants/wikimedia-stream.constants';

declare global {
  var __eventSourceMock: jest.Mock;
  var __eventSourceCapture: { onmessage?: (e: MessageEvent) => void };
}

jest.mock('eventsource', () => {
  let onmessage: ((e: MessageEvent) => void) | undefined;
  const Mock = jest.fn().mockImplementation(function () {
    return {
      close: jest.fn(),
      get onopen() {
        return () => {};
      },
      set onopen(_fn: () => void) {},
      get onerror() {
        return () => {};
      },
      set onerror(_fn: (e: Event) => void) {},
      get onmessage() {
        return onmessage ?? (() => {});
      },
      set onmessage(fn: (e: MessageEvent) => void) {
        onmessage = fn;
      },
    };
  });
  global.__eventSourceMock = Mock;
  (
    global as unknown as {
      __eventSourceCapture: typeof global.__eventSourceCapture;
    }
  ).__eventSourceCapture = {
    get onmessage() {
      return onmessage;
    },
    set onmessage(f) {
      onmessage = f ?? undefined;
    },
  };
  return { __esModule: true, default: Mock, EventSource: Mock };
});

describe('Wikimedia producer (functional)', () => {
  let kafkaEmit: jest.Mock;

  beforeAll(async () => {
    kafkaEmit = jest.fn().mockResolvedValue(undefined);
    const module: TestingModule = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
        WikimediaProducerMicroserviceModule,
      ],
    })
      .overrideProvider(KAFKA_CLIENT)
      .useValue({
        connect: jest.fn().mockResolvedValue(undefined),
        emit: jest.fn().mockReturnValue(of(undefined)),
      })
      .overrideProvider(KafkaProducer)
      .useValue({
        emit: kafkaEmit,
        onModuleInit: jest.fn().mockResolvedValue(undefined),
      })
      .compile();

    const handler = module.get(WikimediaStreamHandler);
    handler.start();
  });

  beforeEach(() => {
    kafkaEmit.mockClear();
  });

  it('should create EventSource with stream URL when stream is started', () => {
    expect(global.__eventSourceMock).toHaveBeenCalledWith(WIKIMEDIA_STREAM_URL);
  });

  it('should emit to Kafka with topic and envelope when SSE message has meta.id', async () => {
    const payload = {
      meta: { id: 'fc-id-1' },
      title: 'Functional test',
      wiki: 'enwiki',
      user: 'TestUser',
    };
    const msg = { data: JSON.stringify(payload) } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();

    expect(kafkaEmit).toHaveBeenCalledTimes(1);
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
        type: EVENT_TYPES.WIKIMEDIA_RECENTCHANGES,
        key: 'fc-id-1',
        payload,
        version: 1,
      }),
    );
  });

  it('should not emit to Kafka when SSE message is invalid JSON', async () => {
    const msg = { data: 'not json' } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();
    expect(kafkaEmit).not.toHaveBeenCalled();
  });
});
