import { credentials, type ServiceError } from '@grpc/grpc-js';
import {
  DataCatalogServiceClient,
  type GetDependencyGraphRequest,
  type GetDependencyGraphResponse,
  type GetRequest,
  type GetResponse,
  type GetTablesRequest,
  type GetTablesResponse,
  type ListRequest,
  type ListResponse,
} from '@/generated/datacatalog/v1/datacatalog';

const GRPC_API_URL = process.env.GRPC_API_URL || 'localhost:30052';

function unary<Req, Res>(
  fn: (req: Req, callback: (err: ServiceError | null, response: Res) => void) => unknown,
  req: Req
): Promise<Res> {
  return new Promise((resolve, reject) => {
    fn(req, (err, res) => {
      if (err) {
        reject(err);
        return;
      }

      resolve(res);
    });
  });
}

let client: DataCatalogServiceClient | undefined;

export function getDataCatalogService() {
  if (!client) {
    client = new DataCatalogServiceClient(GRPC_API_URL, credentials.createInsecure());
  }
  const dataCatalogClient = client;

  return {
    list: (request: ListRequest): Promise<ListResponse> =>
      unary(dataCatalogClient.list.bind(dataCatalogClient), request),
    get: (request: GetRequest): Promise<GetResponse> =>
      unary(dataCatalogClient.get.bind(dataCatalogClient), request),
    getTables: (request: GetTablesRequest): Promise<GetTablesResponse> =>
      unary(dataCatalogClient.getTables.bind(dataCatalogClient), request),
    getDependencyGraph: (request: GetDependencyGraphRequest): Promise<GetDependencyGraphResponse> =>
      unary(dataCatalogClient.getDependencyGraph.bind(dataCatalogClient), request),
  };
}
