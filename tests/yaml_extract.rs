use lidx::indexer::extract::LanguageExtractor;
use lidx::indexer::yaml::{module_name_from_rel_path, YamlExtractor};

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("k8s/deploy.yaml"), "k8s/deploy");
    assert_eq!(module_name_from_rel_path("manifests/service.yml"), "manifests/service");
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
