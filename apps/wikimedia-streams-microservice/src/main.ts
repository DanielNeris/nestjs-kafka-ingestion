import { bootstrapKafkaConsumer } from '@app/kafka';
import { WikimediaStreamsMicroserviceModule } from './wikimedia-streams-microservice.module';

const CONSUMER_GROUP_ID = 'wikimedia-streams-demo';

async function bootstrap() {
  await bootstrapKafkaConsumer(
    WikimediaStreamsMicroserviceModule,
    CONSUMER_GROUP_ID,
  );
}
void bootstrap();
