import { status } from '@grpc/grpc-js';
import { type NextRequest, NextResponse } from 'next/server';
import { getAppInsights } from '@/lib/applicationinsights';
import { parseCatalogItemKind, toCatalogItemType } from '@/lib/catalog-item-kind';
import { getDataCatalogService } from '@/lib/datacatalog-service-client';

export async function GET(request: NextRequest) {
  const uniqueName = request.nextUrl.searchParams.get('uniqueName') ?? '';
  const kind = parseCatalogItemKind(request.nextUrl.searchParams.get('type'));
  if (!kind || !uniqueName.trim() || uniqueName.length > 512) {
    return NextResponse.json({ message: 'Ogiltig katalogpost.' }, { status: 400 });
  }

  try {
    const result = await getDataCatalogService().getTables({
      uniqueName,
      type: toCatalogItemType(kind),
    });
    return NextResponse.json(result, { headers: { 'Cache-Control': 'no-store' } });
  } catch (error) {
    const code = (error as { code?: number }).code;
    if (code === status.INVALID_ARGUMENT) {
      return NextResponse.json({ message: 'Ogiltig katalogpost.' }, { status: 400 });
    }
    if (code === status.NOT_FOUND) {
      return NextResponse.json({ message: 'Katalogposten finns inte längre.' }, { status: 404 });
    }
    getAppInsights().appInsights?.trackException({
      exception: error instanceof Error ? error : new Error('Catalog tables request failed'),
      properties: { feature: 'catalog-tables', source: 'api' },
    });
    return NextResponse.json(
      { message: 'Kunde inte hämta tabeller och kolumner.' },
      { status: 500 }
    );
  }
}
