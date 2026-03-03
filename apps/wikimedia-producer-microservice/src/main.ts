import '@app/kafka/register-snappy';
import { env } from '@app/config';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { WikimediaProducerMicroserviceModule } from './wikimedia-producer-microservice.module';

async function bootstrap() {
  const app = await NestFactory.create(WikimediaProducerMicroserviceModule);
  await app.listen(env.PORT);
  Logger.log(`[WikimediaProducerMicroservice] is running on port ${env.PORT}`);
}
void bootstrap();
