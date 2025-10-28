use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ParamValues, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Set, Statement};
use log::{info, warn};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::ClientInfo;
use pgwire::error::{PgWireError, PgWireResult};
use postgres_types::Type;

use crate::client;
use crate::QueryHook;

#[derive(Debug)]
pub struct SetShowHook;

#[async_trait]
impl QueryHook for SetShowHook {
    /// called in simple query handler to return response directly
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                try_respond_show_statements(client, statement, session_context).await
            }
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        stmt: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let sql_lower = stmt.to_string().to_lowercase();
        let sql_trimmed = sql_lower.trim();

        if sql_trimmed.starts_with("show") {
            let show_schema =
                Arc::new(Schema::new(vec![Field::new("show", DataType::Utf8, false)]));
            let result = show_schema
                .to_dfschema()
                .map(|df_schema| {
                    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(df_schema),
                    })
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)));
            Some(result)
        } else if sql_trimmed.starts_with("set") {
            let show_schema = Arc::new(Schema::new(Vec::<Field>::new()));
            let result = show_schema
                .to_dfschema()
                .map(|df_schema| {
                    LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: true,
                        schema: Arc::new(df_schema),
                    })
                })
                .map_err(|e| PgWireError::ApiError(Box::new(e)));
            Some(result)
        } else {
            None
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set { .. } => {
                try_respond_set_statements(client, statement, session_context).await
            }
            Statement::ShowVariable { .. } | Statement::ShowStatus { .. } => {
                try_respond_show_statements(client, statement, session_context).await
            }
            _ => None,
        }
    }
}

fn mock_show_response(name: &str, value: &str) -> PgWireResult<QueryResponse> {
    let fields = vec![FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )];

    let row = {
        let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
        encoder.encode_field(&Some(value))?;
        encoder.finish()
    };

    let row_stream = futures::stream::once(async move { row });
    Ok(QueryResponse::new(Arc::new(fields), Box::pin(row_stream)))
}

async fn try_respond_set_statements<C>(
    client: &mut C,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + Send + Sync + ?Sized,
{
    let Statement::Set(set_statement) = statement else {
        return None;
    };

    match &set_statement {
        Set::SingleAssignment {
            scope: None,
            hivevar: false,
            variable,
            values,
        } if &variable.to_string() == "statement_timeout" => {
            let value = values[0].to_string();
            let timeout_str = value.trim_matches('"').trim_matches('\'');

            let timeout = if timeout_str == "0" || timeout_str.is_empty() {
                None
            } else {
                // Parse timeout value (supports ms, s, min formats)
                let timeout_ms = if timeout_str.ends_with("ms") {
                    timeout_str.trim_end_matches("ms").parse::<u64>()
                } else if timeout_str.ends_with("s") {
                    timeout_str
                        .trim_end_matches("s")
                        .parse::<u64>()
                        .map(|s| s * 1000)
                } else if timeout_str.ends_with("min") {
                    timeout_str
                        .trim_end_matches("min")
                        .parse::<u64>()
                        .map(|m| m * 60 * 1000)
                } else {
                    // Default to milliseconds
                    timeout_str.parse::<u64>()
                };

                match timeout_ms {
                    Ok(ms) if ms > 0 => Some(std::time::Duration::from_millis(ms)),
                    _ => None,
                }
            };

            client::set_statement_timeout(client, timeout);
            Some(Ok(Response::Execution(Tag::new("SET"))))
        }
        Set::SetTimeZone {
            local: false,
            value,
        } => {
            let tz = value.to_string();
            let tz = tz.trim_matches('"').trim_matches('\'');
            client::set_timezone(client, Some(tz));
            Some(Ok(Response::Execution(Tag::new("SET"))))
        }
        _ => {
            // pass SET query to datafusion
            let query = statement.to_string();
            if let Err(e) = session_context.sql(&query).await {
                warn!("SET statement {query} is not supported by datafusion, error {e}, statement ignored");
            }

            // Always return SET success
            Some(Ok(Response::Execution(Tag::new("SET"))))
        }
    }
}

async fn try_respond_show_statements<C>(
    client: &C,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + ?Sized,
{
    let Statement::ShowVariable { variable } = statement else {
        return None;
    };

    let variables = variable
        .iter()
        .map(|v| &v.value as &str)
        .collect::<Vec<_>>();

    match &variables as &[&str] {
        ["time", "zone"] => {
            let timezone = client::get_timezone(client).unwrap_or("UTC");
            Some(mock_show_response("TimeZone", timezone).map(Response::Query))
        }
        ["server_version"] => {
            Some(mock_show_response("server_version", "15.0 (DataFusion)").map(Response::Query))
        }
        ["transaction_isolation"] => Some(
            mock_show_response("transaction_isolation", "read uncommitted").map(Response::Query),
        ),
        ["catalogs"] => {
            let catalogs = session_context.catalog_names();
            let value = catalogs.join(", ");
            Some(mock_show_response("Catalogs", &value).map(Response::Query))
        }
        ["search_path"] => {
            let default_schema = "public";
            Some(mock_show_response("search_path", default_schema).map(Response::Query))
        }
        ["statement_timeout"] => {
            let timeout = client::get_statement_timeout(client);
            let timeout_str = match timeout {
                Some(duration) => format!("{}ms", duration.as_millis()),
                None => "0".to_string(),
            };
            Some(mock_show_response("statement_timeout", &timeout_str).map(Response::Query))
        }
        ["transaction", "isolation", "level"] => {
            Some(mock_show_response("transaction_isolation", "read uncommitted").map(Response::Query))
        }
        _ => {
            info!("Unsupported show statement: {}", statement);
            Some(mock_show_response("unsupported_show_statement", "").map(Response::Query))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;
    use crate::testing::MockClient;

    #[tokio::test]
    async fn test_statement_timeout_set_and_show() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Test setting timeout to 5000ms
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '5000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let set_response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        assert!(set_response.is_some());
        assert!(set_response.unwrap().is_ok());

        // Verify the timeout was set in client metadata
        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, Some(Duration::from_millis(5000)));

        // Test SHOW statement_timeout
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("show statement_timeout")
            .unwrap()
            .parse_statement()
            .unwrap();
        let show_response =
            try_respond_show_statements(&client, &statement, &session_context).await;

        assert!(show_response.is_some());
        assert!(show_response.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_statement_timeout_disable() {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();

        // Set timeout first
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '1000ms'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        // Disable timeout with 0
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql("set statement_timeout to '0'")
            .unwrap()
            .parse_statement()
            .unwrap();
        let resp = try_respond_set_statements(&mut client, &statement, &session_context).await;
        assert!(resp.is_some());
        assert!(resp.unwrap().is_ok());

        let timeout = client::get_statement_timeout(&client);
        assert_eq!(timeout, None);
    }

    #[tokio::test]
    async fn test_supported_show_statements_returned_columns() {
        let session_context = SessionContext::new();
        let client = MockClient::new();

        let tests = [
            ("show time zone", "TimeZone"),
            ("show server_version", "server_version"),
            ("show transaction_isolation", "transaction_isolation"),
            ("show catalogs", "Catalogs"),
            ("show search_path", "search_path"),
            ("show statement_timeout", "statement_timeout"),
            ("show transaction isolation level", "transaction_isolation"),
        ];

        for (query, expected_response_col) in tests {
            let statement = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(&query)
                .unwrap()
                .parse_statement()
                .unwrap();
            let show_response =
                try_respond_show_statements(&client, &statement, &session_context).await;

            let Some(Ok(Response::Query(show_response))) = show_response else {
                panic!("unexpected show response");
            };

            assert_eq!(show_response.command_tag(), "SELECT");

            let row_schema = show_response.row_schema();
            assert_eq!(row_schema.len(), 1);
            assert_eq!(row_schema[0].name(), expected_response_col);
        }
    }
}
