use super::helpers::*;

const POSTGRES_PUSH_SCHEMA: &str =
    include_str!("../../../../../ops/migrations/postgres/001_push_schema.sql");
const POSTGRES_STATUS_CAS_UPGRADE: &str =
    include_str!("../../../../../ops/migrations/postgres/002_push_status_revision.sql");
const MYSQL_PUSH_SCHEMA: &str =
    include_str!("../../../../../ops/migrations/mysql/003_push_schema.sql");
const MYSQL_STATUS_CAS_UPGRADE: &str =
    include_str!("../../../../../ops/migrations/mysql/004_push_status_revision.sql");

#[test]
fn postgres_query_conversion_numbers_each_placeholder() {
    let sql = sql_query(
        "INSERT INTO t (a, b, c) VALUES (?, ?::jsonb, ?) WHERE ? = ?".to_owned(),
        true,
    );

    assert_eq!(
        sql,
        "INSERT INTO t (a, b, c) VALUES ($1, $2::jsonb, $3) WHERE $4 = $5"
    );
}

#[test]
fn mysql_query_conversion_leaves_placeholders_unchanged() {
    let sql = sql_query("SELECT ? AS a, CAST(? AS JSON) AS b".to_owned(), false);

    assert_eq!(sql, "SELECT ? AS a, CAST(? AS JSON) AS b");
}

#[test]
fn unsigned_mysql_status_metadata_is_selected_as_portable_signed_values() {
    assert_eq!(signed_i64_expr("revision", true), "revision");
    assert_eq!(
        signed_i64_expr("revision", false),
        "CAST(revision AS SIGNED)"
    );
}

#[test]
fn fresh_sql_schemas_include_publish_status_revision_and_version_two() {
    assert_eq!(crate::storage::EXPECTED_PUSH_SCHEMA_VERSION, 2);
    for required in [
        "revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0)",
        "SELECT 2, 0",
        "information_schema.columns",
        "column_name = 'revision'",
    ] {
        assert!(
            POSTGRES_PUSH_SCHEMA.contains(required),
            "postgres fresh schema: {required}"
        );
    }
    for required in [
        "revision BIGINT UNSIGNED NOT NULL DEFAULT 1",
        "SELECT 2, 0",
        "information_schema.columns",
        "column_name = 'revision'",
    ] {
        assert!(
            MYSQL_PUSH_SCHEMA.contains(required),
            "mysql fresh schema: {required}"
        );
    }
}

#[test]
fn sql_publish_status_revision_upgrades_record_version_after_column() {
    let postgres_column = POSTGRES_STATUS_CAS_UPGRADE
        .find("ADD COLUMN revision")
        .expect("postgres upgrade adds revision");
    let postgres_version = POSTGRES_STATUS_CAS_UPGRADE
        .find("VALUES (2, 0)")
        .expect("postgres upgrade records schema version 2");
    assert!(POSTGRES_STATUS_CAS_UPGRADE.contains("BEGIN;"));
    assert!(POSTGRES_STATUS_CAS_UPGRADE.contains("COMMIT;"));
    assert!(postgres_column < postgres_version);

    let mysql_column = MYSQL_STATUS_CAS_UPGRADE
        .find("ADD COLUMN revision")
        .expect("mysql upgrade adds revision");
    let mysql_version = MYSQL_STATUS_CAS_UPGRADE
        .find("VALUES (2, 0)")
        .expect("mysql upgrade records schema version 2");
    assert!(mysql_column < mysql_version);
}
