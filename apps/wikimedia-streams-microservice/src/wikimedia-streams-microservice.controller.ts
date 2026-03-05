import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '@app/contracts';
import { WikimediaStreamsMicroserviceService } from './wikimedia-streams-microservice.service';

@Controller()
export class WikimediaStreamsMicroserviceController {
  constructor(
    private readonly wikimediaStreamsMicroserviceService: WikimediaStreamsMicroserviceService,
  ) {}

  @EventPattern(TOPICS.WIKIMEDIA_RECENTCHANGES)
  handleWikimediaRecentChange(
    @Payload() raw: string | Record<string, unknown>,
  ): void {
    void this.wikimediaStreamsMicroserviceService.handleRecentChange(raw);
  }
}
