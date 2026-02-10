use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::javascript::{JavascriptExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/app.js"), "src/app");
    assert_eq!(module_name_from_rel_path("src/index.js"), "src");
    assert_eq!(module_name_from_rel_path("index.js"), "index");
}

#[test]
fn extract_symbols_and_edges() {
    let source = r#"
import React from "react";
import { foo } from "./lib/foo";
export { bar } from "../bar";

class Base {}

class Foo extends Base {
    constructor() {}
    method(x) { return x; }
}

function util(a, b) { return a + b; }

const MAX = 10;

util(1, 2);
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.qualname.as_str()))
        .collect();

    assert!(names.contains(&("module", "src/app")));
    assert!(names.contains(&("class", "src/app.Base")));
    assert!(names.contains(&("class", "src/app.Foo")));
    assert!(names.contains(&("method", "src/app.Foo.method")));
    assert!(names.contains(&("function", "src/app.util")));
    assert!(names.contains(&("const", "src/app.MAX")));

    let edge_kinds: Vec<_> = extracted.edges.iter().map(|e| e.kind.as_str()).collect();
    assert!(edge_kinds.contains(&"CONTAINS"));
    assert!(edge_kinds.contains(&"IMPORTS"));
    assert!(edge_kinds.contains(&"EXTENDS"));
    assert!(edge_kinds.contains(&"CALLS"));

    let call_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CALLS")
        .collect();
    assert!(
        call_edges
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("src/app.util"))
    );
}

#[test]
fn extract_process_env_config_read() {
    let source = r#"
const dbUrl = process.env.DATABASE_URL;
const apiKey = process.env["API_KEY"];
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }), "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
    config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://API_KEY")
    }), "expected CONFIG_READ for env://API_KEY");
}

#[test]
fn extract_process_env_destructuring() {
    let source = r#"
const { DATABASE_URL, API_KEY } = process.env;
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert_eq!(config_reads.len(), 2, "expected 2 CONFIG_READ edges, found: {:?}",
        config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }));
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://API_KEY")
    }));
}

#[test]
fn extract_process_env_destructuring_renamed() {
    let source = r#"
const { DB_URL: dbUrl } = process.env;
"#;
    let module = module_name_from_rel_path("src/config.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert_eq!(config_reads.len(), 1, "expected 1 CONFIG_READ edge for renamed destructuring, found: {:?}",
        config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DB_URL")
    }));
}

#[test]
fn fastify_direct_route() {
    let source = r#"
const fastify = require('fastify')();
fastify.get('/users', async (req, reply) => {
    return { users: [] };
});
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/users"));
    let detail = routes[0].detail.as_deref().unwrap_or("");
    assert!(detail.contains("fastify"), "expected fastify framework label, got: {detail}");
}

#[test]
fn fastify_register_with_prefix() {
    let source = r#"
const fastify = require('fastify')();
fastify.register((instance, opts, done) => {
    instance.get('/users', async (req, reply) => {
        return [];
    });
    done();
}, { prefix: '/api' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/api/users"));
}

#[test]
fn fastify_nested_register_prefix_stacking() {
    let source = r#"
const fastify = require('fastify')();
fastify.register((app, opts, done) => {
    app.register((inner, opts, done) => {
        inner.get('/users', handler);
        done();
    }, { prefix: '/v1' });
    done();
}, { prefix: '/api' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/api/v1/users"));
}

#[test]
fn fastify_route_object_style() {
    let source = r#"
const app = require('fastify')();
app.route({
    url: '/items',
    method: 'GET',
    handler: async (req, reply) => { return []; }
});
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/items"));
    let detail = routes[0].detail.as_deref().unwrap_or("");
    assert!(detail.contains("fastify"), "expected fastify framework label, got: {detail}");
}

#[test]
fn fastify_register_named_function() {
    let source = r#"
const fastify = require('fastify')();

function userRoutes(instance, opts, done) {
    instance.get('/users', async (req, reply) => {
        return [];
    });
    done();
}

fastify.register(userRoutes, { prefix: '/api' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/api/users"));
}

#[test]
fn fastify_register_const_arrow_function() {
    let source = r#"
const fastify = require('fastify')();

const itemRoutes = async (instance, opts) => {
    instance.get('/items', async (req, reply) => []);
    instance.post('/items', async (req, reply) => {});
};

fastify.register(itemRoutes, { prefix: '/v1' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 2, "expected 2 HTTP_ROUTE edges, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert!(routes.iter().any(|e| e.target_qualname.as_deref() == Some("/v1/items")));
}

#[test]
fn fastify_register_fp_wrapped_inline() {
    let source = r#"
const fp = require('fastify-plugin');
const fastify = require('fastify')();

fastify.register(fp(async function(instance, opts) {
    instance.get('/health', async () => ({ status: 'ok' }));
}), { prefix: '/api' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/api/health"));
}

#[test]
fn fastify_register_fp_wrapped_variable() {
    let source = r#"
const fp = require('fastify-plugin');
const fastify = require('fastify')();

const dbPlugin = fp(async (instance) => {
    instance.get('/db/status', async () => ({ connected: true }));
});

fastify.register(dbPlugin, { prefix: '/internal' });
"#;
    let module = module_name_from_rel_path("src/routes.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE, got {:?}",
        routes.iter().map(|e| (&e.target_qualname, &e.detail)).collect::<Vec<_>>());
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/internal/db/status"));
}

#[test]
fn fastify_db_plugin_config_read() {
    // DB plugin registered from require — can't follow cross-file,
    // but should pick up process.env CONFIG_READ from the options object
    let source = r#"
const fastify = require('fastify')();
fastify.register(require('@fastify/postgres'), {
    connectionString: process.env.DATABASE_URL
});
"#;
    let module = module_name_from_rel_path("src/app.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DATABASE_URL")
    }), "expected CONFIG_READ for DATABASE_URL, got: {:?}",
        config_reads.iter().map(|e| e.target_qualname.as_deref()).collect::<Vec<_>>());
}

#[test]
fn fastify_same_file_plugin_with_routes_and_decorators() {
    // Same-file plugin that adds both decorators and routes
    let source = r#"
const fp = require('fastify-plugin');
const fastify = require('fastify')();

const dbPlugin = fp(async (instance, opts) => {
    const pool = createPool(process.env.DB_CONN);
    instance.decorate('db', pool);
    instance.get('/db/health', async () => ({ ok: true }));
});

async function apiRoutes(app, opts) {
    app.get('/users', async (req) => req.server.db.query('SELECT *'));
    app.post('/users', async (req, reply) => {});
}

fastify.register(dbPlugin);
fastify.register(apiRoutes, { prefix: '/api' });
"#;
    let module = module_name_from_rel_path("src/app.js");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    let route_paths: Vec<_> = routes.iter()
        .map(|e| e.target_qualname.as_deref().unwrap_or(""))
        .collect();
    let route_details: Vec<_> = routes.iter()
        .map(|e| (e.target_qualname.as_deref().unwrap_or(""), e.source_qualname.as_deref().unwrap_or(""), e.evidence_start_line))
        .collect();
    assert!(route_paths.contains(&"/db/health"),
        "expected /db/health route, got: {route_details:?}");
    assert!(route_paths.contains(&"/api/users"),
        "expected /api/users route, got: {route_details:?}");
    // POST /api/users
    assert_eq!(routes.len(), 3, "expected 3 HTTP_ROUTE edges (GET /db/health, GET /api/users, POST /api/users), got: {route_details:?}");

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(config_reads.iter().any(|e| {
        e.target_qualname.as_deref() == Some("env://DB_CONN")
    }), "expected CONFIG_READ for DB_CONN");
}

#[test]
fn fastify_typescript_plugin_function() {
    // Mimics dpb's health.ts pattern: typed params, export default
    let source = r#"
import type { FastifyInstance, FastifyPluginOptions } from 'fastify';

async function healthRoutes(fastify: FastifyInstance, options: FastifyPluginOptions) {
  fastify.get('/health/live', async (request, reply) => {
    return { status: 'ok' };
  });

  fastify.get('/health/ready', async (request, reply) => {
    return { status: 'ready' };
  });

  fastify.get('/health/startup', async (request, reply) => {
    return { status: 'started' };
  });
}

export default healthRoutes;
"#;
    let module = module_name_from_rel_path("node/datacatalog-api/src/routes/health.ts");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    let route_details: Vec<_> = routes.iter()
        .map(|e| (e.target_qualname.as_deref().unwrap_or(""), e.source_qualname.as_deref().unwrap_or(""), e.detail.as_deref().unwrap_or("")))
        .collect();
    assert_eq!(routes.len(), 3, "expected 3 HTTP_ROUTE edges for health routes, got: {route_details:?}");
    assert!(routes.iter().any(|e| e.target_qualname.as_deref() == Some("/health/live")),
        "expected /health/live, got: {route_details:?}");
    assert!(routes.iter().any(|e| e.target_qualname.as_deref() == Some("/health/ready")),
        "expected /health/ready, got: {route_details:?}");
    assert!(routes.iter().any(|e| e.target_qualname.as_deref() == Some("/health/startup")),
        "expected /health/startup, got: {route_details:?}");
    // All routes should have "fastify" framework label
    for r in &routes {
        let detail = r.detail.as_deref().unwrap_or("");
        assert!(detail.contains("fastify"), "expected fastify label, got: {detail}");
    }
}

#[test]
fn fastify_route_inside_exported_function() {
    // Mimics dpb's app.ts: buildApp creates fastify instance and defines a root route
    let source = r#"
import Fastify from 'fastify';

export async function buildApp() {
  const fastify = Fastify({ logger: true });

  fastify.get('/', async (request, reply) => {
    return { status: 'running' };
  });

  return fastify;
}
"#;
    let module = module_name_from_rel_path("node/datacatalog-api/src/app.ts");
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let routes: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "HTTP_ROUTE")
        .collect();
    let route_details: Vec<_> = routes.iter()
        .map(|e| (e.target_qualname.as_deref().unwrap_or(""), e.source_qualname.as_deref().unwrap_or(""), e.detail.as_deref().unwrap_or("")))
        .collect();
    assert_eq!(routes.len(), 1, "expected 1 HTTP_ROUTE for root route, got: {route_details:?}");
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/"));
}
