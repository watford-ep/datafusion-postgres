use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::testing::*;
use datafusion_postgres::DfSessionService;
use pgwire::api::query::SimpleQueryHandler;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn minimal_repro_sort_preserving_merge() {
    let _ = env_logger::try_init();

    let config = SessionConfig::new().with_target_partitions(12);
    let session_ctx = SessionContext::new_with_config(config);
    setup_pg_catalog(&session_ctx, "datafusion", Arc::new(AuthManager::default()))
        .expect("failed to register pg_catalog");

    let service = DfSessionService::new(Arc::new(session_ctx), Arc::new(AuthManager::new()));
    let mut client = MockClient::new();

    let sql = r#"
SELECT ns.nspname, typ.oid, typ.typname, typ.typtype
  FROM pg_type AS typ
  JOIN pg_namespace AS ns ON (ns.oid = typ.typnamespace)
 WHERE typ.typtype IN ('b','r','m','e','d')
 ORDER BY CASE WHEN typ.typtype IN ('b','e','p') THEN 0
               WHEN typ.typtype = 'r' THEN 1
          END
"#;

    match timeout(
        Duration::from_secs(10),
        SimpleQueryHandler::do_query(&service, &mut client, sql),
    )
    .await
    {
        Ok(Ok(_)) => panic!("expected error"),
        Ok(Err(e)) => {
            eprintln!("error: {:?}\n", e);
            let s = format!("{:?}", e);
            assert!(
                s.contains("SanityCheckPlan") || s.contains("SortPreservingMerge"),
                "wrong error: {}",
                s
            );
        }
        Err(_) => panic!("query timed out"),
    }
}
