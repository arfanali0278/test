import { Module } from '@nestjs/common';
import { StroageService } from './stroage.service';
import { StroageController } from './stroage.controller';

@Module({
  providers: [StroageService],
  controllers: [StroageController]
})
export class StroageModule {}
