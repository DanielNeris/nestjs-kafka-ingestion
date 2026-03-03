import { Test, TestingModule } from '@nestjs/testing';
import { EVENT_TYPES, TOPICS } from '@app/contracts';
import { KafkaProducer } from '@app/kafka';
import { WikimediaStreamHandler } from '@app/wikimedia-producer-microservice/wikimedia-stream.handler';
import {
  RECONNECT_DELAY_MS,
  WIKIMEDIA_STREAM_URL,
} from '@app/wikimedia-producer-microservice/constants/wikimedia-stream.constants';

declare global {
  var __eventSourceMock: jest.Mock;

  var __eventSourceMockClose: jest.Mock;

  var __eventSourceCapture: {
    onopen?: () => void;
    onerror?: (e: Event) => void;
    onmessage?: (e: MessageEvent) => void;
  };
}

jest.mock('eventsource', () => {
  const mockCloseFn = jest.fn();
  let onopen: (() => void) | undefined;
  let onerror: ((e: Event) => void) | undefined;
  let onmessage: ((e: MessageEvent) => void) | undefined;
  const Mock = jest.fn().mockImplementation(function (this: unknown) {
    return {
      close: mockCloseFn,
      get onopen(): () => void {
        return onopen ?? (() => {});
      },
      set onopen(fn: () => void) {
        onopen = fn;
      },
      get onerror(): (e: Event) => void {
        return onerror ?? (() => {});
      },
      set onerror(fn: (e: Event) => void) {
        onerror = fn;
      },
      get onmessage(): (e: MessageEvent) => void {
        return onmessage ?? (() => {});
      },
      set onmessage(fn: (e: MessageEvent) => void) {
        onmessage = fn;
      },
    };
  });
  global.__eventSourceMock = Mock;
  global.__eventSourceMockClose = mockCloseFn;
  (
    global as unknown as {
      __eventSourceCapture: typeof global.__eventSourceCapture;
    }
  ).__eventSourceCapture = {
    get onopen() {
      return onopen;
    },
    set onopen(f) {
      onopen = f ?? undefined;
    },
    get onerror() {
      return onerror;
    },
    set onerror(f) {
      onerror = f ?? undefined;
    },
    get onmessage() {
      return onmessage;
    },
    set onmessage(f) {
      onmessage = f ?? undefined;
    },
  } as typeof global.__eventSourceCapture;
  // Support both named import { EventSource } and default import
  return { __esModule: true, default: Mock, EventSource: Mock };
});

describe('WikimediaStreamHandler', () => {
  let handler: WikimediaStreamHandler;
  let kafkaEmit: jest.Mock;

  beforeEach(async () => {
    jest.clearAllMocks();
    if (global.__eventSourceCapture) {
      global.__eventSourceCapture.onopen = undefined;
      global.__eventSourceCapture.onerror = undefined;
      global.__eventSourceCapture.onmessage = undefined;
    }
    kafkaEmit = jest.fn().mockResolvedValue(undefined);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        WikimediaStreamHandler,
        { provide: KafkaProducer, useValue: { emit: kafkaEmit } },
      ],
    }).compile();

    handler = module.get(WikimediaStreamHandler);
    jest.useFakeTimers();
  });

  afterEach(() => {
    handler.stop();
    jest.useRealTimers();
  });

  it('should create EventSource with stream URL and assign handlers on start', () => {
    handler.start();
    expect(global.__eventSourceMock).toHaveBeenCalledWith(WIKIMEDIA_STREAM_URL);
    expect(global.__eventSourceCapture.onopen).toBeDefined();
    expect(global.__eventSourceCapture.onerror).toBeDefined();
    expect(global.__eventSourceCapture.onmessage).toBeDefined();
  });

  it('should close EventSource and clear reference on stop', () => {
    handler.start();
    handler.stop();
    expect(global.__eventSourceMockClose).toHaveBeenCalled();
    handler.stop();
    expect(global.__eventSourceMockClose).toHaveBeenCalledTimes(1);
  });

  it('should not close when onopen is called (connection opens)', () => {
    handler.start();
    const onopen = global.__eventSourceCapture?.onopen;
    if (onopen) onopen();
    expect(global.__eventSourceMockClose).not.toHaveBeenCalled();
  });

  it('should close and reconnect after delay on error', () => {
    handler.start();
    const onerror = global.__eventSourceCapture?.onerror;
    if (onerror) onerror(new Event('error'));
    expect(global.__eventSourceMockClose).toHaveBeenCalled();
    global.__eventSourceMock.mockClear();
    jest.advanceTimersByTime(RECONNECT_DELAY_MS);
    expect(global.__eventSourceMock).toHaveBeenCalledWith(WIKIMEDIA_STREAM_URL);
  });

  it('should parse payload and emit to Kafka with meta.id as key on message', async () => {
    handler.start();
    const id = 'sse-meta-id-123';
    const title = 'Test page title';
    const payload = {
      meta: { id },
      title,
      wiki: 'enwiki',
      user: 'Alice',
    };
    const msg = {
      data: JSON.stringify(payload),
    } as MessageEvent;

    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();

    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        topic: TOPICS.WIKIMEDIA_RECENTCHANGES,
        type: EVENT_TYPES.WIKIMEDIA_RECENTCHANGES,
        key: id,
        payload,
        version: 1,
      }),
    );
  });

  it('should use wiki then title then user then random key when meta.id missing', async () => {
    handler.start();
    const wiki = 'enwiki';
    const payload = { wiki, title: 'Foo', user: 'Bob' };
    const msg = { data: JSON.stringify(payload) } as MessageEvent;

    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();

    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        key: wiki,
        payload,
      }),
    );
  });

  it('should use title as key when meta.id and wiki missing', async () => {
    handler.start();
    const payload = { title: 'OnlyTitle', user: 'U' };
    const msg = { data: JSON.stringify(payload) } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({ key: 'OnlyTitle', payload }),
    );
  });

  it('should use user as key when meta.id, wiki and title missing', async () => {
    handler.start();
    const payload = { user: 'OnlyUser' };
    const msg = { data: JSON.stringify(payload) } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({ key: 'OnlyUser', payload }),
    );
  });

  it('should use random UUID as key when all key fields missing', async () => {
    handler.start();
    const payload = {};
    const msg = { data: JSON.stringify(payload) } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();
    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    expect(kafkaEmit).toHaveBeenCalledWith(
      expect.objectContaining({
        payload,
        key: expect.stringMatching(uuidRegex),
      }),
    );
  });

  it('should be no-op when stop is called and no EventSource exists', () => {
    handler.stop();
    expect(global.__eventSourceMockClose).not.toHaveBeenCalled();
  });

  it('should not throw and should not emit on message parse error', async () => {
    handler.start();
    const msg = { data: 'invalid json' } as MessageEvent;
    global.__eventSourceCapture.onmessage?.(msg);
    await Promise.resolve();
    expect(kafkaEmit).not.toHaveBeenCalled();
  });
});
