import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '@app/contracts';
import { OpensearchConsumerMicroserviceService } from './opensearch-consumer-microservice.service';

@Controller()
export class OpensearchConsumerMicroserviceController {
  constructor(
    private readonly service: OpensearchConsumerMicroserviceService,
  ) {}

  @EventPattern(TOPICS.WIKIMEDIA_RECENTCHANGES)
  handleWikimediaRecentChange(
    @Payload() raw: string | Record<string, unknown>,
  ): void {
    this.service.handleMessage(raw);
  }
}
