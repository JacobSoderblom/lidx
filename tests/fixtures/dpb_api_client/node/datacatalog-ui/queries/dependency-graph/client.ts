import { queryOptions } from '@tanstack/react-query';
import type { GetDependencyGraphResponse } from '@/generated/datacatalog/v1/datacatalog';
import { apiClient } from '@/lib/api-client';
import type { CatalogItemKind } from '@/lib/catalog-item-kind';

export const dependencyGraphQueryOptions = (kind: CatalogItemKind, uniqueName: string) =>
  queryOptions({
    queryKey: ['dependency-graph', kind, uniqueName],
    queryFn: () =>
      apiClient.get<GetDependencyGraphResponse>(
        `/api/dependency-graph?${new URLSearchParams({ type: kind, uniqueName })}`
      ),
    staleTime: 60_000,
    retry: false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
  });
