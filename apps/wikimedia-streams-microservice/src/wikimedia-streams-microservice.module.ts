import { Module } from '@nestjs/common';
import { WikimediaStreamsMicroserviceController } from './wikimedia-streams-microservice.controller';
import { WikimediaStreamsMicroserviceService } from './wikimedia-streams-microservice.service';
import { KafkaModule } from '@app/kafka';
import { ConfigModule } from '@nestjs/config';
import { registerEnv } from '@app/config';

@Module({
  imports: [
    ConfigModule.forRoot({ load: [registerEnv], isGlobal: true }),
    KafkaModule,
  ],
  controllers: [WikimediaStreamsMicroserviceController],
  providers: [WikimediaStreamsMicroserviceService],
})
export class WikimediaStreamsMicroserviceModule {}
