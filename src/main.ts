import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors({
    origin: ['http://localhost:4200'
      ], // Replace with your Angular app's origin
    // You can add additional CORS configuration options here if needed
  });
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
