import { Module } from '@nestjs/common';
import { GetDbController } from './get-db.controller';
import { GetDbService } from './get-db.service';
import { DatabaseService } from '../database.service';
import { DatabaseSqlService } from '../database-sql.service';

@Module({
  controllers: [GetDbController],
  providers: [GetDbService,DatabaseService,DatabaseSqlService],
})
export class GetDbModule {}
