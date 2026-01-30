import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { GetDbModule } from './database/get-db/get-db.module';
import { AzureBlobModule } from './database/azure-blob/azure-blob.module';
import { UsersModule } from './users/users.module';

@Module({
  imports: [GetDbModule,AzureBlobModule, UsersModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
