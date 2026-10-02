import { infiniteQueryOptions } from '@tanstack/react-query';
import type { ListResponse } from '@/generated/datacatalog/v1/datacatalog';
import { apiClient } from '@/lib/api-client';
import { baseListInfiniteQueryOptions, type SearchParams } from './base';

async function fetchListData(params: SearchParams, cursor?: string): Promise<ListResponse> {
  const searchParams = new URLSearchParams(params);

  if (cursor) {
    searchParams.set('cursor', cursor);
  }

  return apiClient.get(`/api/data-products/list?${searchParams.toString()}`);
}

export const listInfiniteQueryOptions = (searchParams: SearchParams) =>
  infiniteQueryOptions({
    ...baseListInfiniteQueryOptions(searchParams),
    queryFn: ({ pageParam }) => fetchListData(searchParams, pageParam),
  });

export type { SearchParams };
