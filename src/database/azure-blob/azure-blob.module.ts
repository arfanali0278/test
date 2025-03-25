import { Module } from '@nestjs/common';
import { AzureBlobController } from './azure-blob.controller';
import { AzureBlobService } from './azure-blob.service';

@Module({
  controllers: [AzureBlobController],
  providers: [AzureBlobService]
})
export class AzureBlobModule {}
