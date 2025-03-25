import { Controller, Get } from '@nestjs/common';
import { GetDbService } from './get-db.service';
import { DatabaseService } from '../database.service';
import { DatabaseSqlService } from '../database-sql.service';

@Controller('db')
export class GetDbController {
  constructor(private readonly getDbService: GetDbService, private readonly databaseService: DatabaseService,
    private databaseSqlService:DatabaseSqlService
  ) { }

  @Get('transfer')
  async transferData() {
    return this.getDbService.transferData();
  }
  @Get('copydatabase')
  async copyDatabase() {
    await this.databaseSqlService.fetchTableRecord();
    // await this.databaseSqlService.transferDatabase();
    // await this.databaseService.restoreDatabase();

    return;
  }
}
