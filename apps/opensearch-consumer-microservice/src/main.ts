import { bootstrapKafkaConsumer } from '@app/kafka';
import { OpensearchConsumerMicroserviceModule } from './opensearch-consumer-microservice.module';

const CONSUMER_GROUP_ID = 'consumer-opensearch-demo';

async function bootstrap() {
  await bootstrapKafkaConsumer(
    OpensearchConsumerMicroserviceModule,
    CONSUMER_GROUP_ID,
  );
}
void bootstrap();
