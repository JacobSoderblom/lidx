import { queryOptions } from '@tanstack/react-query';
import type { GetTablesResponse } from '@/generated/datacatalog/v1/datacatalog';
import { apiClient } from '@/lib/api-client';
import type { CatalogItemKind } from '@/lib/catalog-item-kind';
import { baseTablesQueryOptions } from './base';

export const tablesQueryOptions = (kind: CatalogItemKind, uniqueName: string) =>
  queryOptions({
    ...baseTablesQueryOptions(kind, uniqueName),
    queryFn: () =>
      apiClient.get<GetTablesResponse>(
        `/api/tables?${new URLSearchParams({ type: kind, uniqueName })}`
      ),
  });
