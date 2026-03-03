import { WikimediaStreamHandler } from './wikimedia-stream.handler';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

@Injectable()
export class WikimediaProducerMicroserviceService
  implements OnModuleInit, OnModuleDestroy
{
  constructor(private readonly streamHandler: WikimediaStreamHandler) {}

  onModuleInit(): void {
    this.streamHandler.start();
  }

  onModuleDestroy(): void {
    this.streamHandler.stop();
  }
}
