use super::helpers::*;

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
