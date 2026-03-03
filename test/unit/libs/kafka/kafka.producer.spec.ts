import { Test, TestingModule } from '@nestjs/testing';
import { of } from 'rxjs';
import { KafkaProducer } from '@app/kafka';
import { KAFKA_CLIENT } from '@app/kafka/kafka.constants';
import { TOPICS } from '@app/contracts';

describe('KafkaProducer', () => {
  let producer: KafkaProducer;
  let clientEmit: jest.Mock;
  let connect: jest.Mock;

  beforeEach(async () => {
    connect = jest.fn().mockResolvedValue(undefined);
    clientEmit = jest.fn().mockReturnValue(of(undefined));
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        KafkaProducer,
        {
          provide: KAFKA_CLIENT,
          useValue: {
            connect,
            emit: clientEmit,
          },
        },
      ],
    }).compile();

    producer = module.get(KafkaProducer);
  });

  it('should connect client on module init', async () => {
    await producer.onModuleInit();
    expect(connect).toHaveBeenCalled();
  });

  it('should send envelope with key and payload to topic on emit', async () => {
    await producer.onModuleInit();
    const payload = { id: 'evt-1', title: 'Test event' };
    const key = 'partition-key-1';
    const traceId = 'trace-abc';

    await producer.emit({
      topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
      payload,
      key,
      headers: { 'X-Trace-Id': traceId },
      version: 1,
    });

    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.WIKIMEDIA_RECENTCHANGES,
      expect.objectContaining({
        key,
        value: expect.objectContaining({
          payload,
          version: 1,
          traceId,
        }),
        headers: expect.objectContaining({ 'X-Trace-Id': traceId }),
      }),
    );
  });

  it('should use default version 1 when version is omitted on emit', async () => {
    await producer.onModuleInit();
    await producer.emit({
      topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
      payload: { id: 'evt-2' },
      key: 'key-2',
    });
    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.WIKIMEDIA_RECENTCHANGES,
      expect.objectContaining({
        value: expect.objectContaining({ version: 1 }),
      }),
    );
  });

  it('should work without headers (traceId undefined) on emit', async () => {
    await producer.onModuleInit();
    await producer.emit({
      topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
      payload: { id: 'evt-3' },
      key: 'key-3',
    });
    expect(clientEmit).toHaveBeenCalledWith(
      TOPICS.WIKIMEDIA_RECENTCHANGES,
      expect.objectContaining({
        value: expect.objectContaining({
          traceId: undefined,
          payload: { id: 'evt-3' },
        }),
      }),
    );
  });
});
