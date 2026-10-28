//! Prepared statements follow the cluster's pooler mode, which
//! can be overridden per user or database, not just `[general] pooler_mode`.
//!
//! See <https://github.com/pgdogdev/pgdog/issues/1536>.

use super::prelude::*;
use crate::{
    backend::databases::reload_from_existing,
    config::{PoolerMode, PreparedStatementsLevel, config, load_test_replicas, set},
    net::DataRow,
};
use pgdog_config::ReadWriteSplit;

/// Load a primary + replica cluster with the given `[general]`,
/// per-user and per-database pooler modes.
fn load_replicas(general: PoolerMode, user: Option<PoolerMode>, database: Option<PoolerMode>) {
    load_test_replicas();
    let mut cfg = (*config()).clone();
    cfg.config.general.pooler_mode = general;
    // Reads go only to the replica, writes and unknown queries to the primary.
    cfg.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    cfg.config.general.prepared_statements = PreparedStatementsLevel::Extended;
    for u in &mut cfg.users.users {
        u.pooler_mode = user;
    }
    for db in &mut cfg.config.databases {
        db.pooler_mode = database;
    }
    set(cfg).unwrap();
    reload_from_existing().unwrap();
}

/// Prepare a read on the replica, then execute it with Bind only.
///
/// Without prepared statements tracking, the router can't tell the Bind
/// is a read, sends it to the primary, and Postgres returns 26000
/// (prepared statement does not exist).
async fn prepare_and_execute(client: &mut TestClient) {
    client
        .send(Parse::named("issue_1536", "SELECT 1::bigint"))
        .await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    assert_eq!(
        client.client.prepared_statements.level,
        PreparedStatementsLevel::Extended
    );

    for _ in 0..3 {
        client.send(Bind::new_statement("issue_1536")).await;
        client.send(Execute::new()).await;
        client.send(Sync).await;
        client.try_process().await.unwrap();

        let messages = client.read_until('Z').await.unwrap();
        let row = messages
            .iter()
            .find(|message| message.code() == 'D')
            .expect("data row");
        assert_eq!(
            DataRow::try_from(row.clone()).unwrap().get_int(0, true),
            Some(1)
        );
    }
}

#[tokio::test]
async fn test_user_transaction_mode_tracks_prepared_statements() {
    load_replicas(PoolerMode::Session, Some(PoolerMode::Transaction), None);
    let mut client = TestClient::new(Parameters::default()).await;
    prepare_and_execute(&mut client).await;
}

#[tokio::test]
async fn test_database_transaction_mode_tracks_prepared_statements() {
    load_replicas(PoolerMode::Session, None, Some(PoolerMode::Transaction));
    let mut client = TestClient::new(Parameters::default()).await;
    prepare_and_execute(&mut client).await;
}

#[tokio::test]
async fn test_session_mode_disables_prepared_statements() {
    for (general, user, database) in [
        // Per-user override.
        (PoolerMode::Transaction, Some(PoolerMode::Session), None),
        // Per-database override.
        (PoolerMode::Transaction, None, Some(PoolerMode::Session)),
        // No override.
        (PoolerMode::Session, None, None),
    ] {
        load_replicas(general, user, database);
        let mut client = TestClient::new(Parameters::default()).await;

        client.send_simple(Query::new("SELECT 1")).await;
        client.read_until('Z').await.unwrap();

        assert_eq!(
            client.client.prepared_statements.level,
            PreparedStatementsLevel::Disabled,
            "general: {general:?}, user: {user:?}, database: {database:?}"
        );
    }
}

#[tokio::test]
async fn test_transaction_mode_without_override() {
    load_replicas(PoolerMode::Transaction, None, None);
    let mut client = TestClient::new(Parameters::default()).await;
    prepare_and_execute(&mut client).await;
}
