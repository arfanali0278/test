import { Controller, Post, Get, Param, UploadedFile, UseInterceptors, Res } from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { AzureBlobService } from './azure-blob.service';
import { Response } from 'express';

@Controller('azure-blob')
export class AzureBlobController {
  constructor(private readonly azureBlobService: AzureBlobService) {}

  @Post('upload')
  @UseInterceptors(FileInterceptor('file'))
  async uploadFile(@UploadedFile() file: any) {
    const blobName = await this.azureBlobService.uploadFile(file);
    const url = this.azureBlobService.getBlobUrl(blobName);
    return { blobName, url };
  }

  @Get('download')
  async downloadFile(@Param('blobName') blobName: string, @Res() res: Response) {
    const fileBuffer = await this.azureBlobService.downloadFile('myFile.json');
    res.setHeader('Content-Disposition', `attachment; filename=${blobName}`);
    res.send(fileBuffer);
  }
  @Get('downloadandsaverecord')
  async saveFileData(@Param('blobName') blobName: string, @Res() res: Response) {
    const fileBuffer = await this.azureBlobService.saveFileData('employees_25mb.json');
    res.setHeader('Content-Disposition', `attachment; filename=${blobName}`);
    res.send(fileBuffer);
  }
}
