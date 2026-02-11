"""Tests for SQL query validation."""

import pytest

from src.query_validator import validate_query


class TestValidQueries:
    def test_simple_select(self):
        valid, err = validate_query("SELECT * FROM users")
        assert valid is True
        assert err == ""

    def test_select_with_where(self):
        valid, err = validate_query("SELECT id FROM users WHERE active = true")
        assert valid is True
        assert err == ""

    def test_cte_with_clause(self):
        valid, err = validate_query("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert valid is True
        assert err == ""

    def test_information_schema(self):
        valid, err = validate_query("SELECT * FROM information_schema.columns")
        assert valid is True
        assert err == ""

    def test_keyword_in_column_name(self):
        valid, err = validate_query("SELECT delete_count FROM t")
        assert valid is True
        assert err == ""

    def test_keyword_in_table_name(self):
        valid, err = validate_query("SELECT * FROM update_log")
        assert valid is True
        assert err == ""

    def test_line_comment_stripped(self):
        valid, err = validate_query("-- DROP TABLE\nSELECT 1")
        assert valid is True
        assert err == ""

    def test_block_comment_stripped(self):
        valid, err = validate_query("/* DROP */ SELECT 1")
        assert valid is True
        assert err == ""

    def test_keyword_in_single_quoted_string(self):
        valid, err = validate_query("SELECT * FROM t WHERE op = 'DELETE'")
        assert valid is True
        assert err == ""

    def test_keyword_in_double_quoted_identifier(self):
        valid, err = validate_query('SELECT * FROM "DELETE"')
        assert valid is True
        assert err == ""

    def test_insert_in_string_literal(self):
        valid, err = validate_query("SELECT * FROM events WHERE type = 'INSERT INTO'")
        assert valid is True
        assert err == ""


class TestRejectedQueries:
    def test_empty_query(self):
        valid, err = validate_query("")
        assert valid is False
        assert err == "Empty query"

    def test_whitespace_only(self):
        valid, err = validate_query("   ")
        assert valid is False
        assert err == "Empty query"

    def test_insert(self):
        valid, err = validate_query("INSERT INTO users VALUES (1)")
        assert valid is False

    def test_update(self):
        valid, err = validate_query("UPDATE users SET name = 'x'")
        assert valid is False

    def test_delete(self):
        valid, err = validate_query("DELETE FROM users")
        assert valid is False

    def test_drop(self):
        valid, err = validate_query("DROP TABLE users")
        assert valid is False

    def test_create(self):
        valid, err = validate_query("CREATE TABLE t (id int)")
        assert valid is False

    def test_alter(self):
        valid, err = validate_query("ALTER TABLE t ADD col int")
        assert valid is False

    def test_truncate(self):
        valid, err = validate_query("TRUNCATE TABLE t")
        assert valid is False

    def test_semicolon_with_write(self):
        valid, err = validate_query("SELECT 1; DROP TABLE t")
        assert valid is False
        assert "DROP" in err

    def test_grant(self):
        valid, err = validate_query("GRANT ALL ON users TO public")
        assert valid is False

    def test_revoke(self):
        valid, err = validate_query("REVOKE ALL ON users FROM public")
        assert valid is False

    def test_non_select_start(self):
        valid, err = validate_query("EXPLAIN SELECT 1")
        assert valid is False
        assert "Only SELECT queries are allowed" in err
