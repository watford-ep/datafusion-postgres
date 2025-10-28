use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use postgres_types::Oid;
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::CatalogInfo;

use super::OidCacheKey;

#[derive(Debug, Clone)]
pub(crate) struct PgNamespaceTable<C> {
    schema: SchemaRef,
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
}

impl<C: CatalogInfo> PgNamespaceTable<C> {
    pub(crate) fn new(
        catalog_list: C,
        oid_counter: Arc<AtomicU32>,
        oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    ) -> Self {
        // Define the schema for pg_namespace
        // This matches the columns from PostgreSQL's pg_namespace
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false), // Object identifier
            Field::new("nspname", DataType::Utf8, false), // Name of the namespace (schema)
            Field::new("nspowner", DataType::Int32, false), // Owner of the namespace
            Field::new("nspacl", DataType::Utf8, true), // Access privileges
            Field::new("options", DataType::Utf8, true), // Schema-level options
        ]));

        Self {
            schema,
            catalog_list,
            oid_counter,
            oid_cache,
        }
    }

    /// Generate record batches based on the current state of the catalog
    async fn get_data(this: Self) -> Result<RecordBatch> {
        // Vectors to store column data
        let mut oids = Vec::new();
        let mut nspnames = Vec::new();
        let mut nspowners = Vec::new();
        let mut nspacls: Vec<Option<String>> = Vec::new();
        let mut options: Vec<Option<String>> = Vec::new();

        // to store all schema-oid mapping temporarily before adding to global oid cache
        let mut schema_oid_cache = HashMap::new();

        let mut oid_cache = this.oid_cache.write().await;

        // Now add all schemas from DataFusion catalogs
        for catalog_name in this.catalog_list.catalog_names().await? {
            if let Some(schema_names) = this.catalog_list.schema_names(&catalog_name).await? {
                for schema_name in schema_names {
                    let cache_key = OidCacheKey::Schema(catalog_name.clone(), schema_name.clone());

                    // If schema is the Postgres pg_catalog, assign fixed OID 11 so
                    // other tables that reference pg_catalog by OID continue to work.
                    let schema_oid = if schema_name == "pg_catalog" {
                        let reserved_oid: u32 = 11;

                        let collision = oid_cache.values().any(|v| *v == reserved_oid);
                        if collision {
                            return Err(datafusion::error::DataFusionError::Execution(format!(
                                "reserved OID {} already assigned to another object; cannot assign to pg_catalog",
                                reserved_oid
                            )));
                        } else {
                            // Ensure the global counter is ahead of reserved_oid so it
                            // won't hand out the same value later.
                            loop {
                                let prev = this.oid_counter.load(Ordering::Relaxed);
                                if prev > reserved_oid || this.oid_counter.compare_exchange(prev, reserved_oid + 1, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                                    break;
                                }
                            }
                            reserved_oid
                        }
                    } else if let Some(oid) = oid_cache.get(&cache_key) {
                        *oid
                    } else {
                        this.oid_counter.fetch_add(1, Ordering::Relaxed)
                    };

                    schema_oid_cache.insert(cache_key, schema_oid);

                    oids.push(schema_oid as i32);
                    nspnames.push(schema_name.clone());
                    nspowners.push(10); // Default owner
                    nspacls.push(None);
                    options.push(None);
                }
            }
        }

        // remove all schema cache and table of the schema which is no longer exists
        oid_cache.retain(|key, _| match key {
            OidCacheKey::Catalog(..) => true,
            OidCacheKey::Schema(..) => false,
            OidCacheKey::Table(catalog, schema_name, _) => schema_oid_cache
                .contains_key(&OidCacheKey::Schema(catalog.clone(), schema_name.clone())),
        });
        // add new schema cache
        oid_cache.extend(schema_oid_cache);

        // Create Arrow arrays from the collected data
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(oids)),
            Arc::new(StringArray::from(nspnames)),
            Arc::new(Int32Array::from(nspowners)),
            Arc::new(StringArray::from_iter(nspacls.into_iter())),
            Arc::new(StringArray::from_iter(options.into_iter())),
        ];

        // Create a full record batch
        let batch = RecordBatch::try_new(this.schema.clone(), arrays)?;

        Ok(batch)
    }
}

impl<C: CatalogInfo> PartitionStream for PgNamespaceTable<C> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let this = self.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            this.schema.clone(),
            futures::stream::once(async move { Self::get_data(this).await }),
        ))
    }
}
