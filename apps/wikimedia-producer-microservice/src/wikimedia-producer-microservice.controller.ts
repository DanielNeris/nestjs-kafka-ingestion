import { Controller, Get } from '@nestjs/common';
import { WikimediaProducerMicroserviceService } from './wikimedia-producer-microservice.service';

@Controller()
export class WikimediaProducerMicroserviceController {
  constructor(
    private readonly wikimediaProducerMicroserviceService: WikimediaProducerMicroserviceService,
  ) {}

  @Get()
  onModuleInit(): void {
    void this.wikimediaProducerMicroserviceService.onModuleInit();
  }
}
