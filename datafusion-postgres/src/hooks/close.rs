use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use pgwire::api::results::{Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ToDFSchema;
use pgwire::error::PgWireError;

use crate::QueryHook;

#[derive(Debug)]
pub struct CloseHook;

#[async_trait]
impl QueryHook for CloseHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Close { .. } => Some(Ok(Response::Execution(Tag::new("CLOSE CURSOR ALL")))),
            _ => return None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match stmt {
            Statement::Close { .. } => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "close",
                    DataType::Utf8,
                    false,
                )]));
                let result = schema
                    .to_dfschema()
                    .map(|df_schema| {
                        LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                            produce_one_row: true,
                            schema: Arc::new(df_schema),
                        })
                    })
                    .map_err(|e| PgWireError::ApiError(Box::new(e)));
                Some(result)
            }
            _ => return None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Close { .. } => Some(Ok(Response::Execution(Tag::new("CLOSE CURSOR ALL")))),
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use crate::testing::MockClient;

    #[tokio::test]
    async fn test_close_statement() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();
        let hook = CloseHook;

        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("CLOSE some_cursor")
            .unwrap()
            .parse_statement()
            .unwrap();

        let response = hook
            .handle_simple_query(&statement, &session_context, &mut client)
            .await;

        assert!(response.is_some());
        let response = response.unwrap();
        assert!(response.is_ok());

        match response.unwrap() {
            Response::Execution(tag) => {
                assert_eq!(tag, Tag::new("CLOSE CURSOR ALL"));
            }
            _ => panic!("Expected Execution response"),
        }
    }

    #[tokio::test]
    async fn test_close_all_statement() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();
        let hook = CloseHook;

        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("CLOSE ALL")
            .unwrap()
            .parse_statement()
            .unwrap();

        let response = hook
            .handle_simple_query(&statement, &session_context, &mut client)
            .await;

        assert!(response.is_some());
        let response = response.unwrap();
        assert!(response.is_ok());

        match response.unwrap() {
            Response::Execution(tag) => {
                assert_eq!(tag, Tag::new("CLOSE CURSOR ALL"));
            }
            _ => panic!("Expected Execution response"),
        }
    }
}
