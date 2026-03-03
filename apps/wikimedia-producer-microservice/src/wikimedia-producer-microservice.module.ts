import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';
import { KafkaModule } from '@app/kafka';
import { WikimediaProducerMicroserviceController } from './wikimedia-producer-microservice.controller';
import { WikimediaProducerMicroserviceService } from './wikimedia-producer-microservice.service';
import { WikimediaStreamHandler } from './wikimedia-stream.handler';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [WikimediaProducerMicroserviceController],
  providers: [WikimediaStreamHandler, WikimediaProducerMicroserviceService],
})
export class WikimediaProducerMicroserviceModule {}
