from techmart.ops.pg_write import PgTableSpec, pg_ddl, pg_type
from techmart.spark.framework import SparkColumn

_SPEC = PgTableSpec(
    schema="ops",
    name="widget",
    grain="one row per widget",
    columns=[
        SparkColumn("widget_id", "long", "Surrogate key", is_key=True, nullable=False),
        SparkColumn("qty", "int", "Units", nullable=False),
        SparkColumn("note", "string", "Free text"),
        SparkColumn("created_at", "timestamp", "When", nullable=False),
    ],
    primary_key=("widget_id",),
)


def test_pg_type_mapping():
    assert pg_type("long") == "bigint"
    assert pg_type("int") == "integer"
    assert pg_type("double") == "double precision"
    assert pg_type("string") == "text"
    assert pg_type("boolean") == "boolean"
    assert pg_type("timestamp") == "timestamptz"
    assert pg_type("date") == "date"


def test_pg_ddl_create_pk_and_types():
    create = pg_ddl(_SPEC, "techmart_ops")[0]
    assert "CREATE TABLE IF NOT EXISTS techmart_ops.widget" in create
    assert "widget_id bigint NOT NULL" in create
    assert "qty integer NOT NULL" in create
    assert "created_at timestamptz NOT NULL" in create
    # nullable column has no NOT NULL
    assert "note text" in create and "note text NOT NULL" not in create
    assert "PRIMARY KEY (widget_id)" in create


def test_pg_ddl_comments():
    joined = "\n".join(pg_ddl(_SPEC, "techmart_ops"))
    assert "COMMENT ON TABLE techmart_ops.widget IS 'one row per widget';" in joined
    assert "COMMENT ON COLUMN techmart_ops.widget.widget_id IS 'Surrogate key';" in joined
    assert "COMMENT ON COLUMN techmart_ops.widget.note IS 'Free text';" in joined


def test_pg_ddl_escapes_single_quotes():
    spec = PgTableSpec(
        schema="ops", name="t", grain="grain's test",
        columns=[SparkColumn("id", "long", "it's a key", nullable=False)],
        primary_key=("id",),
    )
    joined = "\n".join(pg_ddl(spec, "s"))
    assert "IS 'grain''s test';" in joined
    assert "IS 'it''s a key';" in joined


def test_column_names():
    assert _SPEC.column_names == ["widget_id", "qty", "note", "created_at"]
