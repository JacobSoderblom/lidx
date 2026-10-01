use lidx::indexer::extract::{LanguageExtractor, ReceiverType};
use lidx::indexer::javascript::{
    JavascriptExtractor, TsxExtractor, TypescriptExtractor, module_name_from_rel_path,
};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("src/app.js"), "src/app");
    assert_eq!(module_name_from_rel_path("src/index.js"), "src");
    assert_eq!(module_name_from_rel_path("index.js"), "index");
}

// Receiver-type-inference regression tests (mirrors the Python mechanism's
// own test shapes; see `python::infer_receiver_type`'s doc comment). Each
// of these fails if the corresponding change in `javascript.rs` is
// reverted: `this_method_resolves_to_enclosing_class` and
// `bare_function_call_still_resolves` only prove `receiver_type` stays
// `NotTracked` in cases already exact/unresolved-receiver-free before this
// change; `typed_parameter_method_resolves_to_declared_type` and
// `untyped_receiver_does_not_bind` are the discriminating ones — both
// assert a `receiver_type` value (`Known(..)` / `Unresolved`) that the
// pre-change extractor could never produce (every edge defaulted to
// `NotTracked`).

#[test]
fn this_method_resolves_to_enclosing_class() {
    let source = r#"
class Foo {
    helper() {}
    method() {
        this.helper();
    }
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("src/app.Foo.helper"))
        .expect("this.helper() call edge");
    assert_eq!(call.receiver_type, ReceiverType::NotTracked);
}

#[test]
fn typed_parameter_method_resolves_to_declared_type() {
    let source = r#"
class Foo {
    method(store: EventStore) {
        store.append(1);
    }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.append"))
        .expect("store.append() call edge");
    assert_eq!(
        call.receiver_type,
        ReceiverType::Known("EventStore".to_string())
    );
}

#[test]
fn untyped_receiver_does_not_bind() {
    let source = r#"
class Foo {
    method(store) {
        store.append(1);
    }
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.append"))
        .expect("store.append() call edge");
    assert_eq!(call.receiver_type, ReceiverType::Unresolved);
}

#[test]
fn bare_function_call_still_resolves() {
    let source = r#"
function helper() {}
function main() {
    helper();
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("src/app.helper"))
        .expect("helper() call edge");
    assert_eq!(call.receiver_type, ReceiverType::NotTracked);
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
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") }),
        "expected CONFIG_READ for env://DATABASE_URL, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://API_KEY") }),
        "expected CONFIG_READ for env://API_KEY"
    );
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
    assert_eq!(
        config_reads.len(),
        2,
        "expected 2 CONFIG_READ edges, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") })
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://API_KEY") })
    );
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
    assert_eq!(
        config_reads.len(),
        1,
        "expected 1 CONFIG_READ edge for renamed destructuring, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DB_URL") })
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/users"));
    let detail = routes[0].detail.as_deref().unwrap_or("");
    assert!(
        detail.contains("fastify"),
        "expected fastify framework label, got: {detail}"
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/items"));
    let detail = routes[0].detail.as_deref().unwrap_or("");
    assert!(
        detail.contains("fastify"),
        "expected fastify framework label, got: {detail}"
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
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
    assert_eq!(
        routes.len(),
        2,
        "expected 2 HTTP_ROUTE edges, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
    assert!(
        routes
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("/v1/items"))
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
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
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE, got {:?}",
        routes
            .iter()
            .map(|e| (&e.target_qualname, &e.detail))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        routes[0].target_qualname.as_deref(),
        Some("/internal/db/status")
    );
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
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") }),
        "expected CONFIG_READ for DATABASE_URL, got: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );
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
    let route_paths: Vec<_> = routes
        .iter()
        .map(|e| e.target_qualname.as_deref().unwrap_or(""))
        .collect();
    let route_details: Vec<_> = routes
        .iter()
        .map(|e| {
            (
                e.target_qualname.as_deref().unwrap_or(""),
                e.source_qualname.as_deref().unwrap_or(""),
                e.evidence_start_line,
            )
        })
        .collect();
    assert!(
        route_paths.contains(&"/db/health"),
        "expected /db/health route, got: {route_details:?}"
    );
    assert!(
        route_paths.contains(&"/api/users"),
        "expected /api/users route, got: {route_details:?}"
    );
    // POST /api/users
    assert_eq!(
        routes.len(),
        3,
        "expected 3 HTTP_ROUTE edges (GET /db/health, GET /api/users, POST /api/users), got: {route_details:?}"
    );

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DB_CONN") }),
        "expected CONFIG_READ for DB_CONN"
    );
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
    let route_details: Vec<_> = routes
        .iter()
        .map(|e| {
            (
                e.target_qualname.as_deref().unwrap_or(""),
                e.source_qualname.as_deref().unwrap_or(""),
                e.detail.as_deref().unwrap_or(""),
            )
        })
        .collect();
    assert_eq!(
        routes.len(),
        3,
        "expected 3 HTTP_ROUTE edges for health routes, got: {route_details:?}"
    );
    assert!(
        routes
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("/health/live")),
        "expected /health/live, got: {route_details:?}"
    );
    assert!(
        routes
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("/health/ready")),
        "expected /health/ready, got: {route_details:?}"
    );
    assert!(
        routes
            .iter()
            .any(|e| e.target_qualname.as_deref() == Some("/health/startup")),
        "expected /health/startup, got: {route_details:?}"
    );
    // All routes should have "fastify" framework label
    for r in &routes {
        let detail = r.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("fastify"),
            "expected fastify label, got: {detail}"
        );
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
    let route_details: Vec<_> = routes
        .iter()
        .map(|e| {
            (
                e.target_qualname.as_deref().unwrap_or(""),
                e.source_qualname.as_deref().unwrap_or(""),
                e.detail.as_deref().unwrap_or(""),
            )
        })
        .collect();
    assert_eq!(
        routes.len(),
        1,
        "expected 1 HTTP_ROUTE for root route, got: {route_details:?}"
    );
    assert_eq!(routes[0].target_qualname.as_deref(), Some("/"));
}

#[test]
fn multiline_chained_call_resolves_like_single_line() {
    let source = "
function caller() {
    UniqueName
        .Create();
}
";
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.detail.is_none())
        .expect("UniqueName.Create() call edge");
    assert_eq!(
        call.target_qualname.as_deref(),
        Some("UniqueName.Create"),
        "multi-line chain must resolve to the same qualname as the single-line form"
    );
}

// Regression tests for the walker returning at every arrow-function boundary
// (`is_dynamic_this_function_node`'s predecessor stopped there too) without
// ever descending into the body — meaning a call written inside `.map()`,
// `.then()`, `useEffect(() => ...)`, etc. produced no CALLS edge anywhere,
// no matter what it was attributed to. Mirrors the C# `csharp_extract.rs`
// pair (`call_inside_lambda_body_attributes_to_enclosing_method` /
// `lambda_parameter_shadowing_field_does_not_bind_to_field_type`) — a
// lambda body is a nested scope, not a new symbol.

#[test]
fn call_inside_arrow_function_body_attributes_to_enclosing_method() {
    let source = r#"
class Foo {
    helper(item) {}
    method(items) {
        items.forEach(item => {
            this.helper(item);
        });
    }
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("src/app.Foo.helper"))
        .expect("this.helper(item) call edge inside the forEach arrow body");
    assert_eq!(
        call.source_qualname.as_deref(),
        Some("src/app.Foo.method"),
        "a call inside an arrow function is a nested scope, not a new symbol \
         — it must attribute to the enclosing named method, and `this` must \
         still resolve to the enclosing class (arrow functions never rebind \
         `this`)"
    );
}

#[test]
fn typed_local_resolves_inside_arrow_function_body() {
    let source = r#"
class Foo {
    method(store: EventStore, promise: Promise<void>) {
        promise.then(() => {
            store.append(1);
        });
    }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.append"))
        .expect("store.append(1) call edge inside the .then() arrow body");
    assert_eq!(
        call.receiver_type,
        ReceiverType::Known("EventStore".to_string()),
        "the enclosing method's typed parameter must still resolve from inside the arrow body"
    );
}

#[test]
fn arrow_function_parameter_shadowing_local_does_not_bind_to_declared_type() {
    let source = r#"
class Foo {
    method(store: EventStore) {
        withStore((store) => {
            store.append(1);
        });
    }
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("store.append"))
        .expect("store.append(1) call edge inside the arrow body");
    assert_eq!(
        call.receiver_type,
        ReceiverType::Unresolved,
        "the arrow function's own (untyped) parameter shadows the outer \
         `store: EventStore` and must not be resolved via the outer type"
    );
}

// Issue #111: `<Foo />` usage emitted no edge at all — `walk_node` only fed
// a `jsx_element`/`jsx_self_closing_element` node to `jsx_route_edge`
// (react-router `<Route path="...">` detection only), so every React
// component's actual JSX usages were invisible to the graph and every
// component came back with 0 callers. A capitalized JSX tag name is a
// reference to an in-scope value/component (never a literal DOM tag — see
// https://react.dev/learn/your-first-component#using-a-component), so it
// must be resolved through the same import path an ordinary call uses.

#[test]
fn jsx_self_closing_capitalized_component_emits_calls_edge() {
    let source = r#"
import { StatusBadge } from "./status-badge";

function ProductTabs() {
    return <StatusBadge status="active" />;
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/product-tabs").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| {
            e.kind == "CALLS"
                && e.target_qualname.as_deref() == Some("src/product-tabs.StatusBadge")
        })
        .expect("<StatusBadge /> usage must emit a CALLS edge");
    assert_eq!(
        call.import_candidates,
        vec!["./status-badge\0StatusBadge".to_string()],
        "the JSX usage must resolve through imports exactly like an ordinary call"
    );
}

#[test]
fn jsx_element_with_children_capitalized_component_emits_calls_edge() {
    let source = r#"
import { Card } from "./card";

function Page() {
    return <Card>content</Card>;
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/page").unwrap();
    assert!(
        extracted
            .edges
            .iter()
            .any(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("src/page.Card")),
        "<Card>...</Card> usage must emit a CALLS edge, got {:?}",
        extracted
            .edges
            .iter()
            .filter(|e| e.kind == "CALLS")
            .collect::<Vec<_>>()
    );
}

#[test]
fn jsx_namespaced_capitalized_component_emits_calls_edge() {
    let source = r#"
import * as ui from "./ui";

function Menu() {
    return <ui.Foo />;
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/menu").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("ui.Foo"))
        .expect("<ui.Foo /> usage must emit a CALLS edge");
    assert_eq!(
        call.import_candidates,
        vec!["./ui\0Foo".to_string()],
        "a namespace-qualified JSX usage must resolve through imports like `ui.Foo()` would"
    );
}

#[test]
fn jsx_lowercase_intrinsic_tags_emit_no_calls_edge() {
    let source = r#"
function Layout() {
    return <div className="wrap"><span /></div>;
}
"#;
    let mut extractor = JavascriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/layout").unwrap();
    assert!(
        extracted.edges.iter().all(|e| e.kind != "CALLS"),
        "lowercase intrinsic JSX tags (<div>, <span>) must not emit CALLS edges, got {:?}",
        extracted
            .edges
            .iter()
            .filter(|e| e.kind == "CALLS")
            .collect::<Vec<_>>()
    );
}

#[test]
fn tsx_imported_component_call_resolves_through_imports() {
    let source = r#"
import { StatusBadge } from './status-badge';

export function Card() {
    return <StatusBadge status="active" />;
}
"#;
    let mut extractor = TsxExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/card").unwrap();
    let call = extracted
        .edges
        .iter()
        .find(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("src/card.StatusBadge"))
        .expect("<StatusBadge /> usage in tsx file must emit a CALLS edge with import candidates");
    assert_eq!(
        call.import_candidates,
        vec!["./status-badge\0StatusBadge".to_string()],
        "JSX component in .tsx file must resolve through imports like a function call"
    );
}

#[test]
fn call_results_assigned_to_variables_do_not_become_grpc_clients() {
    let source = r#"
import { jobScheduling } from './api';
import { client } from './client';

async function processData() {
    // Function call results should not be registered as gRPC clients
    const jobs = await jobScheduling.listJobs({ filter: 'active' });
    jobs.map(x => x.id);
    jobs.filter(x => x.status === 'pending');
    jobs.forEach(x => console.log(x));

    // Same for non-grpc fetch
    const response = await client.getCatalogItem({ id: '123' });
    response.json();
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let rpc_calls: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "RPC_CALL")
        .collect();
    assert!(
        rpc_calls.is_empty(),
        "Variables assigned from function call results must not become gRPC clients, got RPC_CALL edges: {:?}",
        rpc_calls
    );
}

#[test]
fn explicit_grpc_client_still_emits_rpc_call() {
    let source = r#"
import { FooServiceClient } from './foo_grpc_pb';

async function main() {
    const client = new FooServiceClient('localhost:50051');
    client.someMethod({ request: 'data' });
}
"#;
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    let rpc_calls: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "RPC_CALL")
        .collect();
    assert!(
        !rpc_calls.is_empty(),
        "Explicit gRPC client call must emit RPC_CALL edges, got no RPC_CALL edges"
    );
    let call = rpc_calls[0];
    assert!(
        call.target_qualname
            .as_deref()
            .map(|q| q.contains("somemethod"))
            .unwrap_or(false),
        "gRPC client call should resolve to the method, got: {:?}",
        call.target_qualname
    );
}

fn rpc_calls_for(source: &str) -> Vec<lidx::indexer::extract::EdgeInput> {
    let mut extractor = TypescriptExtractor::new().unwrap();
    let extracted = extractor.extract(source, "src/app").unwrap();
    extracted
        .edges
        .into_iter()
        .filter(|e| e.kind == "RPC_CALL")
        .collect()
}

#[test]
fn builtin_and_library_constructors_are_not_grpc_clients() {
    let source = r#"
import sql from 'mssql';
import { Repository } from './repo';
import { Client } from 'pg';

function run() {
    const m = new Map<string, string>();
    m.set('a', 'b');
    const s = new Set([1]);
    s.has(1);
    const d = new Date();
    d.getTime();
    const p = new Promise((r) => r(1));
    p.then(() => {});
    const e = new Error('x');
    e.toString();
    const r = new RegExp('a');
    r.test('a');
    const u = new URL('http://x');
    u.toString();
    const q = new sql.Request();
    q.input('a', 1);
    const repo = new Repository();
    repo.find();
    const pg = new Client();
    pg.connect();
    new Map().get('k');
    new sql.Request().query('x');
}
"#;
    let calls = rpc_calls_for(source);
    assert!(calls.is_empty(), "no RPC_CALL expected, got {calls:?}");
}

fn assert_rpc_targets(source: &str, wants: &[&str]) {
    let targets: Vec<_> = rpc_calls_for(source)
        .iter()
        .filter_map(|e| e.target_qualname.clone())
        .collect();
    for want in wants {
        assert!(
            targets.iter().any(|t| t == want),
            "missing {want} in {targets:?}"
        );
    }
}

#[test]
fn grpc_client_from_stub_namespace_import_emits_rpc_call() {
    let source = r#"
import * as pb from './greeter_grpc_pb';
function run() { const b = new pb.GreeterClient('h'); b.sayHello({}); }
"#;
    assert_rpc_targets(source, &["/greeter/sayhello"]);
}

#[test]
fn grpc_client_named_import_from_grpc_pb_emits_rpc_call() {
    let source = r#"
import { FooServiceClient } from './foo_grpc_pb';
function run() { const a = new FooServiceClient('h'); a.doFoo({}); }
"#;
    assert_rpc_targets(source, &["/fooservice/dofoo"]);
}

#[test]
fn grpc_client_from_required_stub_emits_rpc_call() {
    let source = r#"
const { BarClient } = require('./bar_grpc_pb');
function run() { const c = new BarClient('h'); c.doBar({}); }
"#;
    assert_rpc_targets(source, &["/bar/dobar"]);
}

#[test]
fn grpc_client_under_load_package_definition_root_emits_rpc_call() {
    let source = r#"
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const pkgDef = protoLoader.loadSync('x.proto');
const hello = grpc.loadPackageDefinition(pkgDef).helloworld;
function run() { const d = new hello.Greeter('h'); d.sayHi({}); }
"#;
    assert_rpc_targets(source, &["/hello.greeter/sayhi"]);
}

#[test]
fn grpc_generic_client_constructor_local_emits_rpc_call_for_literal_service() {
    let source = r#"
import * as grpc from '@grpc/grpc-js';
const Generic = grpc.makeGenericClientConstructor({}, 'Svc');
function run() { const c = new Generic('h'); c.doGeneric({}); }
"#;
    assert_rpc_targets(source, &["/svc/dogeneric"]);
}

#[test]
fn grpc_generic_client_from_service_local_emits_rpc_call() {
    let source = r#"
import * as grpc from '@grpc/grpc-js';
const Gen2 = grpc.makeGenericClientFromService(svc as any, {});
function run() { const d = new Gen2('h'); d.doGen2({}); }
"#;
    assert_rpc_targets(source, &["/gen2/dogen2"]);
}

#[test]
fn ts_proto_client_impl_import_emits_name_only_rpc_call() {
    let source = r#"
import { UserServiceClientImpl } from './user';
function run() { const a = new UserServiceClientImpl(rpc); a.getUser({}); }
"#;
    assert_rpc_targets(source, &["/userservice/getuser"]);
}

#[test]
fn grpcish_module_accepts_any_capitalised_constructor() {
    let source = r#"
import { Greeter } from './generated/greeter';
function run() { const h = new Greeter('h'); h.sayHi({}); }
"#;
    assert_rpc_targets(source, &["/greeter/sayhi"]);
}

#[test]
fn neutral_module_client_is_flagged_name_only() {
    let source = r#"
import { OrderServiceClient } from './order';
import { FooServiceClient } from './foo_grpc_pb';
function run() {
    const a = new OrderServiceClient('h'); a.placeOrder({});
    const b = new FooServiceClient('h'); b.doFoo({});
}
"#;
    let calls = rpc_calls_for(source);
    let detail_of = |target: &str| -> String {
        calls
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some(target))
            .and_then(|e| e.detail.clone())
            .unwrap_or_else(|| panic!("missing {target}"))
    };
    assert!(detail_of("/orderservice/placeorder").contains("\"evidence\":\"name\""));
    assert!(!detail_of("/fooservice/dofoo").contains("evidence"));
}

#[test]
fn grpc_runtime_package_classes_and_create_client_are_not_clients() {
    let source = r#"
import * as grpc from '@grpc/grpc-js';
import { Server } from '@grpc/grpc-js';
import { createClient } from 'nice-grpc';
import { UserService } from './gen/user';
function run() {
    const s = new Server(); s.start();
    const k = new grpc.Client('h', creds); k.makeUnaryRequest();
    const i = createClient(UserService, channel); i.getUser({});
}

"#;
    let calls = rpc_calls_for(source);
    assert!(calls.is_empty(), "no RPC_CALL expected, got {calls:?}");
}
