/**
 * Client-side API client
 * Used by Client Components
 * Talks to Next.js API routes (same origin, no CORS)
 */

'use client';

interface RequestOptions extends RequestInit {
  timeout?: number;
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public data?: unknown
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

async function fetchWithTimeout(url: string, options: RequestOptions = {}): Promise<Response> {
  const { timeout = 10000, ...fetchOptions } = options;

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await fetch(url, {
      ...fetchOptions,
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error instanceof Error && error.name === 'AbortError') {
      throw new ApiError(408, 'Request timeout');
    }
    throw error;
  }
}

export async function apiClientFetch<T>(
  endpoint: string,
  options: RequestOptions = {}
): Promise<T> {
  // Endpoint should start with /api/
  const url = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;

  try {
    const response = await fetchWithTimeout(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new ApiError(
        response.status,
        errorData.message || `API error: ${response.statusText}`,
        errorData
      );
    }

    return response.json();
  } catch (error) {
    if (error instanceof ApiError) {
      throw error;
    }
    throw new ApiError(500, error instanceof Error ? error.message : 'Unknown error');
  }
}

// Convenience methods
export const apiClient = {
  get: <T>(endpoint: string, options?: RequestOptions) =>
    apiClientFetch<T>(endpoint, { ...options, method: 'GET' }),

  post: <T>(endpoint: string, data?: unknown, options?: RequestOptions) =>
    apiClientFetch<T>(endpoint, {
      ...options,
      method: 'POST',
      body: JSON.stringify(data),
    }),

  put: <T>(endpoint: string, data?: unknown, options?: RequestOptions) =>
    apiClientFetch<T>(endpoint, {
      ...options,
      method: 'PUT',
      body: JSON.stringify(data),
    }),

  patch: <T>(endpoint: string, data?: unknown, options?: RequestOptions) =>
    apiClientFetch<T>(endpoint, {
      ...options,
      method: 'PATCH',
      body: JSON.stringify(data),
    }),

  delete: <T>(endpoint: string, options?: RequestOptions) =>
    apiClientFetch<T>(endpoint, { ...options, method: 'DELETE' }),
};
