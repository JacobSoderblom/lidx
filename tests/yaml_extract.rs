use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::yaml::{YamlExtractor, module_name_from_rel_path};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("k8s/deploy.yaml"), "k8s/deploy");
    assert_eq!(
        module_name_from_rel_path("manifests/service.yml"),
        "manifests/service"
    );
    assert_eq!(module_name_from_rel_path("pod.yaml"), "pod");
}

#[test]
fn extract_single_deployment() {
    let source = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-server
  namespace: production
spec:
  template:
    spec:
      containers:
        - name: api
          image: myregistry/api:v1.2.3
        - name: sidecar
          image: envoyproxy/envoy:v1.28
"#;
    let module = module_name_from_rel_path("k8s/deploy.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| (s.kind.as_str(), s.name.as_str(), s.qualname.as_str()))
        .collect();

    // Module symbol
    assert!(names.contains(&("module", "deploy", "k8s/deploy")));
    // Resource symbol
    assert!(names.contains(&(
        "deployment",
        "api-server",
        "k8s://production/deployment/api-server"
    )));
    // Container symbols
    assert!(names.contains(&(
        "container",
        "api",
        "k8s://production/deployment/api-server/container/api"
    )));
    assert!(names.contains(&(
        "container",
        "sidecar",
        "k8s://production/deployment/api-server/container/sidecar"
    )));

    // Check signatures
    let deploy_sym = extracted
        .symbols
        .iter()
        .find(|s| s.kind == "deployment")
        .unwrap();
    assert_eq!(
        deploy_sym.signature.as_deref(),
        Some("apps/v1 Deployment production/api-server")
    );
    let api_container = extracted
        .symbols
        .iter()
        .find(|s| s.kind == "container" && s.name == "api")
        .unwrap();
    assert_eq!(
        api_container.signature.as_deref(),
        Some("myregistry/api:v1.2.3")
    );

    // CONTAINS edges: module->deployment, deployment->containers
    let contains_edges: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONTAINS")
        .collect();
    assert!(contains_edges.iter().any(|e| {
        e.source_qualname.as_deref() == Some("k8s/deploy")
            && e.target_qualname.as_deref() == Some("k8s://production/deployment/api-server")
    }));
    assert!(contains_edges.iter().any(|e| {
        e.source_qualname.as_deref() == Some("k8s://production/deployment/api-server")
            && e.target_qualname.as_deref()
                == Some("k8s://production/deployment/api-server/container/api")
    }));
}

#[test]
fn extract_multi_document() {
    let source = r#"apiVersion: v1
kind: Service
metadata:
  name: api-svc
spec:
  ports:
    - port: 80
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-server
spec:
  template:
    spec:
      containers:
        - name: api
          image: nginx
"#;
    let module = module_name_from_rel_path("k8s/app.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let kinds: Vec<_> = extracted
        .symbols
        .iter()
        .filter(|s| s.kind != "module" && s.kind != "container")
        .map(|s| s.kind.as_str())
        .collect();
    assert!(kinds.contains(&"service"));
    assert!(kinds.contains(&"deployment"));

    let names: Vec<_> = extracted
        .symbols
        .iter()
        .map(|s| s.qualname.as_str())
        .collect();
    assert!(names.contains(&"k8s://default/service/api-svc"));
    assert!(names.contains(&"k8s://default/deployment/api-server"));
}

#[test]
fn non_k8s_yaml_returns_module_only() {
    let source = r#"name: CI Pipeline
on:
  push:
    branches: [main]
jobs:
  build:
    runs-on: ubuntu-latest
"#;
    let module = module_name_from_rel_path(".github/workflows/ci.yml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert_eq!(extracted.symbols.len(), 1);
    assert_eq!(extracted.symbols[0].kind, "module");
    assert!(extracted.edges.is_empty());
}

#[test]
fn default_namespace_when_absent() {
    let source = r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
data:
  key: value
"#;
    let module = module_name_from_rel_path("k8s/config.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let configmap = extracted
        .symbols
        .iter()
        .find(|s| s.kind == "configmap")
        .unwrap();
    assert_eq!(configmap.qualname, "k8s://default/configmap/app-config");
}

#[test]
fn extract_cronjob_containers() {
    let source = r#"apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-backup
  namespace: ops
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: backup
              image: backup-tool:latest
"#;
    let module = module_name_from_rel_path("k8s/cronjob.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let container = extracted
        .symbols
        .iter()
        .find(|s| s.kind == "container")
        .unwrap();
    assert_eq!(
        container.qualname,
        "k8s://ops/cronjob/nightly-backup/container/backup"
    );
    assert_eq!(container.signature.as_deref(), Some("backup-tool:latest"));
}

#[test]
fn docstring_includes_labels() {
    let source = r#"apiVersion: v1
kind: Service
metadata:
  name: web
  labels:
    app: frontend
    tier: web
"#;
    let module = module_name_from_rel_path("k8s/svc.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let svc = extracted
        .symbols
        .iter()
        .find(|s| s.kind == "service")
        .unwrap();
    let doc = svc.docstring.as_deref().unwrap();
    assert!(doc.contains("app=frontend"));
    assert!(doc.contains("tier=web"));
}

#[test]
fn extract_deployment_env_secret_key_ref() {
    let source = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: datamgr
spec:
  template:
    spec:
      containers:
        - name: datamgr
          image: datamgr:latest
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: datamgr-db-conn
                  key: connection-string
            - name: APP_PORT
              value: "8080"
"#;
    let module = module_name_from_rel_path("k8s/deploy.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // CONFIG_SOURCE for DATABASE_URL env var
    let config_sources: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_SOURCE")
        .collect();
    assert!(
        config_sources
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://DATABASE_URL") }),
        "expected CONFIG_SOURCE for env://DATABASE_URL, found: {:?}",
        config_sources
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );

    // CONFIG_READ for secret://datamgr-db-conn
    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("secret://datamgr-db-conn") }),
        "expected CONFIG_READ for secret://datamgr-db-conn, found: {:?}",
        config_reads
            .iter()
            .map(|e| e.target_qualname.as_deref())
            .collect::<Vec<_>>()
    );

    // CONFIG_SOURCE for APP_PORT (plain value)
    assert!(
        config_sources
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("env://APP_PORT") }),
        "expected CONFIG_SOURCE for env://APP_PORT"
    );
}

#[test]
fn extract_deployment_env_from_secret_ref() {
    let source = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  template:
    spec:
      containers:
        - name: api
          image: api:latest
          envFrom:
            - secretRef:
                name: api-secrets
"#;
    let module = module_name_from_rel_path("k8s/deploy.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("secret://api-secrets") }),
        "expected CONFIG_READ for secret://api-secrets"
    );
}

#[test]
fn extract_secret_provider_class() {
    let source = r#"apiVersion: secrets-store.csi.x-k8s.io/v1
kind: SecretProviderClass
metadata:
  name: azure-kv-secrets
spec:
  provider: azure
  parameters:
    objects: |
      - objectName: datamgr-db-conn
        objectType: secret
      - objectName: api-key
        objectType: secret
  secretObjects:
    - secretName: datamgr-secrets
      type: Opaque
      data:
        - objectName: datamgr-db-conn
          key: connection-string
"#;
    let module = module_name_from_rel_path("k8s/spc.yaml");
    let mut extractor = YamlExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // CONFIG_READ for each objectName in parameters.objects
    let config_reads: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_READ")
        .collect();
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("secret://datamgr-db-conn") }),
        "expected CONFIG_READ for secret://datamgr-db-conn"
    );
    assert!(
        config_reads
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("secret://api-key") }),
        "expected CONFIG_READ for secret://api-key"
    );

    // CONFIG_SOURCE for secretObjects[].secretName
    let config_sources: Vec<_> = extracted
        .edges
        .iter()
        .filter(|e| e.kind == "CONFIG_SOURCE")
        .collect();
    assert!(
        config_sources
            .iter()
            .any(|e| { e.target_qualname.as_deref() == Some("secret://datamgr-secrets") }),
        "expected CONFIG_SOURCE for secret://datamgr-secrets"
    );
}

// --- Span accuracy (#229) ---

const SPAN_FIXTURE: &str = r#"apiVersion: v1
kind: Service
metadata:
  name: svc
---
# second document
apiVersion: apps/v1
kind: Deployment
metadata:
  name: datamgr
spec:
  template:
    spec:
      initContainers:
        - name: generate-cert
          image: busybox
          env:
            - name: Database__ConnectionString
              value: init
      containers:
        - name: datamgr
          image: datamgr:1
          env:
            - name: Database__ConnectionString
              valueFrom:
                secretKeyRef:
                  name: db-secret
                  key: conn
            - name: LOG_LEVEL
              value: info
"#;

fn lines_of(source: &str, start: i64, end: i64) -> String {
    source
        .lines()
        .skip((start - 1) as usize)
        .take((end - start + 1) as usize)
        .collect::<Vec<_>>()
        .join("\n")
}

fn extract_span_fixture() -> lidx::indexer::extract::ExtractedFile {
    let mut extractor = YamlExtractor::new().unwrap();
    extractor.extract(SPAN_FIXTURE, "k8s/spans").unwrap()
}

#[test]
fn spans_never_exceed_file_length() {
    let total = SPAN_FIXTURE.lines().count() as i64;
    let out = extract_span_fixture();
    for s in &out.symbols {
        assert!(s.end_line <= total, "{} ends at {}", s.qualname, s.end_line);
        assert!(s.end_byte <= SPAN_FIXTURE.len() as i64);
    }
    for e in &out.edges {
        if let Some(end) = e.evidence_end_line {
            assert!(end <= total, "edge ends at {end}");
        }
    }
}

#[test]
fn container_spans_are_distinct_and_narrow() {
    let out = extract_span_fixture();
    let find = |name: &str| {
        out.symbols
            .iter()
            .find(|s| s.kind == "container" && s.name == name)
            .unwrap()
    };
    let init = find("generate-cert");
    let main = find("datamgr");
    assert!(init.end_line < main.start_line, "spans overlap");
    let init_text = lines_of(SPAN_FIXTURE, init.start_line, init.end_line);
    assert!(init_text.contains("generate-cert") && !init_text.contains("datamgr:1"));
    assert!(init_text.trim_start().starts_with("- name: generate-cert"));
    let main_text = lines_of(SPAN_FIXTURE, main.start_line, main.end_line);
    assert!(main_text.contains("LOG_LEVEL") && !main_text.contains("generate-cert"));
    // byte range matches the line range
    let bytes = &SPAN_FIXTURE[main.start_byte as usize..main.end_byte as usize];
    assert!(bytes.contains("datamgr:1") && !bytes.contains("busybox"));
}

#[test]
fn second_document_spans_are_absolute() {
    let out = extract_span_fixture();
    let dep = out.symbols.iter().find(|s| s.kind == "deployment").unwrap();
    assert_eq!(dep.start_line, 7);
    assert!(
        SPAN_FIXTURE
            .lines()
            .nth(6)
            .unwrap()
            .starts_with("apiVersion: apps/v1")
    );
    assert_eq!(dep.end_line, SPAN_FIXTURE.lines().count() as i64);
}

#[test]
fn module_span_unchanged() {
    let out = extract_span_fixture();
    let module = out.symbols.iter().find(|s| s.kind == "module").unwrap();
    assert_eq!(module.start_line, 1);
    assert_eq!(module.end_line, SPAN_FIXTURE.lines().count() as i64);
}

#[test]
fn env_edges_have_per_entry_evidence() {
    let out = extract_span_fixture();
    let init_q = "k8s://default/deployment/datamgr/container/generate-cert";
    let main_q = "k8s://default/deployment/datamgr/container/datamgr";
    let env_edges = |q: &str| -> Vec<_> {
        out.edges
            .iter()
            .filter(|e| {
                e.source_qualname.as_deref() == Some(q)
                    && e.target_qualname.as_deref() == Some("env://DATABASE__CONNECTIONSTRING")
            })
            .collect()
    };
    let init_edges = env_edges(init_q);
    let main_edges = env_edges(main_q);
    assert_eq!(init_edges.len(), 1);
    assert_eq!(main_edges.len(), 1);
    let (a, b) = (init_edges[0], main_edges[0]);
    assert_ne!(a.evidence_start_line, b.evidence_start_line);
    let a_text = lines_of(
        SPAN_FIXTURE,
        a.evidence_start_line.unwrap(),
        a.evidence_end_line.unwrap(),
    );
    assert!(a_text.contains("value: init") && !a_text.contains("secretKeyRef"));
    let b_text = lines_of(
        SPAN_FIXTURE,
        b.evidence_start_line.unwrap(),
        b.evidence_end_line.unwrap(),
    );
    assert!(b_text.contains("secretKeyRef") && !b_text.contains("LOG_LEVEL"));
    assert!(
        a.evidence_snippet
            .as_deref()
            .is_some_and(|s| s.contains("Database__ConnectionString"))
    );
    assert!(b.evidence_snippet.as_deref().is_some_and(|s| !s.is_empty()));

    // CONFIG_READ from secretKeyRef covers just the secretKeyRef block
    let read = out
        .edges
        .iter()
        .find(|e| {
            e.kind == "CONFIG_READ" && e.target_qualname.as_deref() == Some("secret://db-secret")
        })
        .unwrap();
    let read_text = lines_of(
        SPAN_FIXTURE,
        read.evidence_start_line.unwrap(),
        read.evidence_end_line.unwrap(),
    );
    assert!(read_text.trim_start().starts_with("secretKeyRef:"));
    assert!(read_text.contains("key: conn") && !read_text.contains("LOG_LEVEL"));
    assert!(
        read.evidence_snippet
            .as_deref()
            .is_some_and(|s| s.contains("secretKeyRef"))
    );
}
