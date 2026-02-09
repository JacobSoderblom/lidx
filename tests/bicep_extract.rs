use lidx::indexer::bicep::{module_name_from_rel_path, BicepExtractor};
use lidx::indexer::extract::LanguageExtractor;

#[test]
fn module_name_from_path() {
    assert_eq!(module_name_from_rel_path("infra/bicep/main.bicep"), "infra/bicep/main");
    assert_eq!(module_name_from_rel_path("infra/main.bicepparam"), "infra/main");
    assert_eq!(module_name_from_rel_path("main.bicep"), "main");
}

#[test]
fn extract_resource() {
    let source = r#"resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'myKeyVault'
  location: location
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
  }
}
"#;
    let module = module_name_from_rel_path("infra/bicep/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // Module + resource
    assert!(extracted.symbols.iter().any(|s| s.kind == "module" && s.qualname == "infra/bicep/main"));
    let res = extracted.symbols.iter().find(|s| s.kind == "resource").unwrap();
    assert_eq!(res.name, "keyVault");
    assert_eq!(res.qualname, "infra/bicep/main.keyVault");
    assert_eq!(res.signature.as_deref(), Some("Microsoft.KeyVault/vaults@2023-07-01"));

    // CONTAINS edge
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "CONTAINS"
            && e.source_qualname.as_deref() == Some("infra/bicep/main")
            && e.target_qualname.as_deref() == Some("infra/bicep/main.keyVault")
    }));
}

#[test]
fn extract_module_ref() {
    let source = r#"module logAnalytics 'modules/logAnalytics.bicep' = {
  name: 'logAnalyticsDeployment'
  params: {
    location: location
  }
}
"#;
    let module = module_name_from_rel_path("infra/bicep/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let mod_ref = extracted.symbols.iter().find(|s| s.kind == "module_ref").unwrap();
    assert_eq!(mod_ref.name, "logAnalytics");
    assert_eq!(mod_ref.qualname, "infra/bicep/main.logAnalytics");
    assert_eq!(mod_ref.signature.as_deref(), Some("modules/logAnalytics.bicep"));

    // IMPORTS_FILE edge
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "IMPORTS_FILE"
            && e.source_qualname.as_deref() == Some("infra/bicep/main.logAnalytics")
            && e.target_qualname.as_deref() == Some("modules/logAnalytics.bicep")
    }));
}

#[test]
fn extract_params_vars_outputs() {
    let source = r#"param location string = 'eastus'
param serverName string
var defaultTags = {
  environment: 'prod'
}
output vaultUri string = keyVault.properties.vaultUri
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let param_loc = extracted.symbols.iter().find(|s| s.kind == "param" && s.name == "location").unwrap();
    assert_eq!(param_loc.signature.as_deref(), Some("string"));
    assert_eq!(param_loc.qualname, "infra/main.location");

    let param_server = extracted.symbols.iter().find(|s| s.kind == "param" && s.name == "serverName").unwrap();
    assert_eq!(param_server.signature.as_deref(), Some("string"));

    let var = extracted.symbols.iter().find(|s| s.kind == "var").unwrap();
    assert_eq!(var.name, "defaultTags");
    assert!(var.signature.is_none());

    let output = extracted.symbols.iter().find(|s| s.kind == "output").unwrap();
    assert_eq!(output.name, "vaultUri");
    assert_eq!(output.signature.as_deref(), Some("string"));
}

#[test]
fn extract_existing_resource() {
    let source = "resource existingVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {\n  name: 'myVault'\n}\n";
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let res = extracted.symbols.iter().find(|s| s.kind == "resource").unwrap();
    assert_eq!(res.name, "existingVault");
    assert_eq!(
        res.signature.as_deref(),
        Some("Microsoft.KeyVault/vaults@2023-07-01 existing")
    );
}

#[test]
fn extract_conditional_resource() {
    let source = r#"resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = if (deployStorage) {
  name: storageName
  location: location
  kind: 'StorageV2'
  sku: {
    name: 'Standard_LRS'
  }
}
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let res = extracted.symbols.iter().find(|s| s.kind == "resource").unwrap();
    assert_eq!(res.name, "storageAccount");
    assert_eq!(
        res.signature.as_deref(),
        Some("Microsoft.Storage/storageAccounts@2023-01-01")
    );
}

#[test]
fn extract_for_loop_resource() {
    let source = r#"resource secrets 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = [for secret in secretList: {
  parent: keyVault
  name: secret.name
  properties: {
    value: secret.value
  }
}]
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let res = extracted.symbols.iter().find(|s| s.kind == "resource").unwrap();
    assert_eq!(res.name, "secrets");
    assert_eq!(
        res.signature.as_deref(),
        Some("Microsoft.KeyVault/vaults/secrets@2023-07-01")
    );
}

#[test]
fn extract_bicepparam() {
    let source = r#"using '../main.bicep'

param location = 'eastus'
param serverName = 'myserver'
var suffix = 'prod'
"#;
    let module = module_name_from_rel_path("infra/envs/prod.bicepparam");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // Module symbol
    assert!(extracted.symbols.iter().any(|s| s.kind == "module" && s.qualname == "infra/envs/prod"));

    // IMPORTS_FILE edge from using
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "IMPORTS_FILE"
            && e.source_qualname.as_deref() == Some("infra/envs/prod")
            && e.target_qualname.as_deref() == Some("../main.bicep")
    }));

    // Params
    let params: Vec<_> = extracted.symbols.iter().filter(|s| s.kind == "param").collect();
    assert_eq!(params.len(), 2);

    // Var
    assert!(extracted.symbols.iter().any(|s| s.kind == "var" && s.name == "suffix"));
}

#[test]
fn decorator_description_as_docstring() {
    let source = r#"@description('The Azure region for deployment')
param location string = 'eastus'
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let param = extracted.symbols.iter().find(|s| s.kind == "param").unwrap();
    assert_eq!(param.docstring.as_deref(), Some("The Azure region for deployment"));
}

#[test]
fn secure_param() {
    let source = r#"@secure()
param adminPassword string
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let param = extracted.symbols.iter().find(|s| s.kind == "param").unwrap();
    assert_eq!(param.name, "adminPassword");
    assert_eq!(param.signature.as_deref(), Some("string (secure)"));
}

#[test]
fn extract_type_and_func() {
    let source = r#"type storageSkuType = 'Standard_LRS' | 'Standard_GRS'
func buildUrl(host string, path string) string => '${host}/${path}'
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    let type_sym = extracted.symbols.iter().find(|s| s.kind == "type").unwrap();
    assert_eq!(type_sym.name, "storageSkuType");
    assert_eq!(type_sym.qualname, "infra/main.storageSkuType");

    let func_sym = extracted.symbols.iter().find(|s| s.kind == "function").unwrap();
    assert_eq!(func_sym.name, "buildUrl");
    assert_eq!(func_sym.qualname, "infra/main.buildUrl");
    // Signature should include params and return type
    assert!(func_sym.signature.is_some());
    let sig = func_sym.signature.as_deref().unwrap();
    assert!(sig.contains("host"));
    assert!(sig.contains("path"));
}

#[test]
fn comments_dont_break_parsing() {
    let source = r#"// This is a comment
param location string

/* Block comment
   spanning multiple
   lines */
var name = 'test'

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  // inline comment
  name: 'kv'
}
"#;
    let module = module_name_from_rel_path("infra/main.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // Should extract all 3 declarations despite comments
    assert!(extracted.symbols.iter().any(|s| s.kind == "param" && s.name == "location"));
    assert!(extracted.symbols.iter().any(|s| s.kind == "var" && s.name == "name"));
    assert!(extracted.symbols.iter().any(|s| s.kind == "resource" && s.name == "kv"));
}

#[test]
fn realistic_module_file() {
    let source = r#"targetScope = 'resourceGroup'

metadata description = 'Service Bus namespace and queues'

@description('The Azure region')
param location string

param namespaceName string

var defaultSku = 'Standard'

resource serviceBusNamespace 'Microsoft.ServiceBus/namespaces@2022-10-01-preview' = {
  name: namespaceName
  location: location
  sku: {
    name: defaultSku
  }
}

resource deadLetterQueue 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBusNamespace
  name: 'dead-letter'
  properties: {
    maxDeliveryCount: 10
  }
}

module monitoring 'modules/monitoring.bicep' = {
  name: 'monitoringDeployment'
  params: {
    namespaceName: serviceBusNamespace.name
  }
}

output namespaceId string = serviceBusNamespace.id
"#;
    let module = module_name_from_rel_path("infra/bicep/serviceBus.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // File module with targetScope signature and metadata description docstring
    let file_mod = extracted.symbols.iter().find(|s| s.kind == "module").unwrap();
    assert_eq!(file_mod.signature.as_deref(), Some("resourceGroup"));
    assert_eq!(file_mod.docstring.as_deref(), Some("Service Bus namespace and queues"));

    // Params
    let loc_param = extracted.symbols.iter().find(|s| s.kind == "param" && s.name == "location").unwrap();
    assert_eq!(loc_param.docstring.as_deref(), Some("The Azure region"));

    // Var
    assert!(extracted.symbols.iter().any(|s| s.kind == "var" && s.name == "defaultSku"));

    // Resources
    let resources: Vec<_> = extracted.symbols.iter().filter(|s| s.kind == "resource").collect();
    assert_eq!(resources.len(), 2);
    assert!(resources.iter().any(|s| s.name == "serviceBusNamespace"));
    assert!(resources.iter().any(|s| s.name == "deadLetterQueue"));

    // Module ref
    let mod_ref = extracted.symbols.iter().find(|s| s.kind == "module_ref").unwrap();
    assert_eq!(mod_ref.name, "monitoring");

    // Output
    let out = extracted.symbols.iter().find(|s| s.kind == "output").unwrap();
    assert_eq!(out.name, "namespaceId");
    assert_eq!(out.signature.as_deref(), Some("string"));

    // Total symbols: module + 2 params + 1 var + 2 resources + 1 module_ref + 1 output = 8
    assert_eq!(extracted.symbols.len(), 8);

    // CONTAINS edges for all non-module symbols
    let contains: Vec<_> = extracted.edges.iter().filter(|e| e.kind == "CONTAINS").collect();
    assert_eq!(contains.len(), 7); // 2 params + 1 var + 2 resources + 1 module_ref + 1 output

    // IMPORTS_FILE edge for module ref
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "IMPORTS_FILE"
            && e.source_qualname.as_deref() == Some("infra/bicep/serviceBus.monitoring")
    }));

    // deadLetterQueue is a SB queue with name: 'dead-letter' → channel edges
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_PUBLISH"
            && e.source_qualname.as_deref() == Some("infra/bicep/serviceBus.deadLetterQueue")
            && e.target_qualname.as_deref() == Some("channel://deadletter")
    }));
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_SUBSCRIBE"
            && e.source_qualname.as_deref() == Some("infra/bicep/serviceBus.deadLetterQueue")
            && e.target_qualname.as_deref() == Some("channel://deadletter")
    }));

    // serviceBusNamespace uses an expression (namespaceName), not a literal — no channel edges
    assert!(!extracted.edges.iter().any(|e| {
        (e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE")
            && e.source_qualname.as_deref() == Some("infra/bicep/serviceBus.serviceBusNamespace")
    }));
}

#[test]
fn channel_edges_for_service_bus_topic() {
    let source = r#"resource topicOrchestratorTriggers 'Microsoft.ServiceBus/namespaces/topics@2022-10-01-preview' = {
  parent: serviceBusNamespace
  name: 'sbt-orchestrator-triggers'
  properties: {
    maxSizeInMegabytes: 1024
  }
}
"#;
    let module = module_name_from_rel_path("infra/bicep/topics.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    // Should emit both CHANNEL_PUBLISH and CHANNEL_SUBSCRIBE
    let pub_edge = extracted.edges.iter().find(|e| e.kind == "CHANNEL_PUBLISH").unwrap();
    assert_eq!(pub_edge.source_qualname.as_deref(), Some("infra/bicep/topics.topicOrchestratorTriggers"));
    assert_eq!(pub_edge.target_qualname.as_deref(), Some("channel://orchestratortriggers"));
    assert!(pub_edge.detail.as_ref().unwrap().contains("\"framework\":\"azure-service-bus\""));
    assert!(pub_edge.detail.as_ref().unwrap().contains("\"role\":\"infrastructure\""));
    assert!(pub_edge.detail.as_ref().unwrap().contains("\"raw\":\"sbt-orchestrator-triggers\""));

    let sub_edge = extracted.edges.iter().find(|e| e.kind == "CHANNEL_SUBSCRIBE").unwrap();
    assert_eq!(sub_edge.source_qualname.as_deref(), Some("infra/bicep/topics.topicOrchestratorTriggers"));
    assert_eq!(sub_edge.target_qualname.as_deref(), Some("channel://orchestratortriggers"));
}

#[test]
fn channel_edges_for_service_bus_queue() {
    let source = r#"resource queueIngest 'Microsoft.ServiceBus/namespaces/queues@2022-10-01-preview' = {
  parent: serviceBusNamespace
  name: 'sbq-data-ingest'
  properties: {
    maxDeliveryCount: 5
  }
}
"#;
    let module = module_name_from_rel_path("infra/bicep/queues.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert!(extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_PUBLISH"
            && e.target_qualname.as_deref() == Some("channel://dataingest")
    }));
    assert!(extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_SUBSCRIBE"
            && e.target_qualname.as_deref() == Some("channel://dataingest")
    }));
}

#[test]
fn no_channel_edges_for_non_servicebus() {
    let source = r#"resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: 'kv-my-vault'
  location: location
}
"#;
    let module = module_name_from_rel_path("infra/bicep/kv.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert!(!extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE"
    }));
}

#[test]
fn no_channel_edges_for_topic_subscription() {
    let source = r#"resource topicSubscription 'Microsoft.ServiceBus/namespaces/topics/subscriptions@2022-10-01-preview' = {
  parent: topic
  name: 'sbts-my-subscription'
  properties: {
    maxDeliveryCount: 10
  }
}
"#;
    let module = module_name_from_rel_path("infra/bicep/subs.bicep");
    let mut extractor = BicepExtractor::new().unwrap();
    let extracted = extractor.extract(source, &module).unwrap();

    assert!(!extracted.edges.iter().any(|e| {
        e.kind == "CHANNEL_PUBLISH" || e.kind == "CHANNEL_SUBSCRIBE"
    }));
}
