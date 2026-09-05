# Databricks notebook source

# COMMAND ----------
# ============================================================
# 01_OLTP_Bronze_Ingestion
# ============================================================
#
# PURPOSE
# -------
# Ingest raw OLTP CSV files directly from a Unity Catalog Volume
# into auditable, append-preserving Bronze Delta tables.
#
# ARCHITECTURE
# ------------
#
# /Volumes/mercury/bronze/raw_files
#                  |
#                  v
#        Explicit-schema CSV read
#                  |
#                  v
#        Source integrity checks
#                  |
#                  v
#       Metadata + record hashing
#                  |
#                  v
#          Bronze Delta tables
#                  |
#                  v
#       Bronze ingestion audit
#
#
# IMPORTANT BRONZE PRINCIPLES
# ---------------------------
#
# 1. Raw source values are preserved.
# 2. No business cleansing occurs here.
# 3. All source attributes remain STRING.
# 4. Bad values such as "TWO" or "NOT_AVAILABLE" survive intact.
# 5. Datatype failures are AUDITED, not corrected.
# 6. Replaying the same source batch does not create duplicates.
# 7. The same physical record arriving with different content
#    causes a pipeline failure.
#
# ============================================================


from __future__ import annotations

import csv
import json
import re
import uuid

from datetime import datetime, timezone

from delta.tables import DeltaTable

from pyspark import StorageLevel

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# COMMAND ----------
# ============================================================
# 1. SPARK SESSION CONFIGURATION
# ============================================================

# Pipeline timestamps should not depend on whoever happens to be
# running the notebook or which cluster timezone is configured.

spark.conf.set(
    "spark.sql.session.timeZone",
    "UTC",
)


# COMMAND ----------
# ============================================================
# 2. NOTEBOOK WIDGET HELPERS
# ============================================================

# Databricks widgets persist between notebook executions.
#
# We therefore only create a widget if it does not already exist.
# This is important because later we will change B001 -> B002 and
# do not want a notebook rerun to reset it accidentally.


def widget_exists(
    widget_name: str,
) -> bool:
    try:
        dbutils.widgets.get(
            widget_name
        )
        return True

    except Exception:
        return False


def ensure_text_widget(
    name: str,
    default_value: str,
    label: str,
) -> None:

    if not widget_exists(name):
        dbutils.widgets.text(
            name,
            default_value,
            label,
        )


def ensure_dropdown_widget(
    name: str,
    default_value: str,
    choices: list[str],
    label: str,
) -> None:

    if not widget_exists(name):
        dbutils.widgets.dropdown(
            name,
            default_value,
            choices,
            label,
        )


# COMMAND ----------
# ============================================================
# 3. PIPELINE PARAMETERS
# ============================================================

# These defaults match your actual UST environment.

ensure_text_widget(
    "catalog",
    "mercury",
    "Catalog",
)

ensure_text_widget(
    "schema",
    "bronze",
    "Schema",
)

ensure_text_widget(
    "volume_name",
    "raw_files",
    "Raw Files Volume",
)

ensure_text_widget(
    "source_batch_id",
    "B001",
    "Source Batch ID",
)

ensure_text_widget(
    "source_system",
    "OLTP_RETAIL",
    "Source System",
)

ensure_dropdown_widget(
    "allow_additive_schema_evolution",
    "false",
    [
        "false",
        "true",
    ],
    "Allow Additive Schema Evolution",
)


CATALOG = (
    dbutils.widgets
    .get("catalog")
    .strip()
)

SCHEMA = (
    dbutils.widgets
    .get("schema")
    .strip()
)

VOLUME_NAME = (
    dbutils.widgets
    .get("volume_name")
    .strip()
)

SOURCE_BATCH_ID = (
    dbutils.widgets
    .get("source_batch_id")
    .strip()
    .upper()
)

SOURCE_SYSTEM = (
    dbutils.widgets
    .get("source_system")
    .strip()
)

ALLOW_ADDITIVE_SCHEMA_EVOLUTION = (
    dbutils.widgets
    .get(
        "allow_additive_schema_evolution"
    )
    .strip()
    .lower()
    == "true"
)


# Unique identifier for THIS execution of the notebook.
#
# B001 identifies the source delivery.
# PIPELINE_RUN_ID identifies this particular pipeline execution.

PIPELINE_RUN_ID = str(
    uuid.uuid4()
)


# COMMAND ----------
# ============================================================
# 4. IDENTIFIER VALIDATION
# ============================================================

IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*$"
)


def validate_identifier(
    value: str,
    label: str,
) -> str:

    if not IDENTIFIER_PATTERN.fullmatch(
        value
    ):
        raise ValueError(
            f"Invalid {label}: {value!r}"
        )

    return value


validate_identifier(
    CATALOG,
    "catalog",
)

validate_identifier(
    SCHEMA,
    "schema",
)

validate_identifier(
    VOLUME_NAME,
    "volume",
)


if not re.fullmatch(
    r"[A-Za-z0-9_]+",
    SOURCE_BATCH_ID,
):
    raise ValueError(
        "SOURCE_BATCH_ID contains "
        "unsupported characters."
    )


if not SOURCE_SYSTEM:
    raise ValueError(
        "SOURCE_SYSTEM cannot be empty."
    )


# COMMAND ----------
# ============================================================
# 5. UNITY CATALOG CONTEXT
# ============================================================

spark.sql(
    f"USE CATALOG `{CATALOG}`"
)

spark.sql(
    f"USE SCHEMA `{SCHEMA}`"
)


context = (
    spark.sql(
        """
        SELECT
            current_catalog() AS catalog,
            current_schema() AS schema
        """
    )
    .first()
)


print("=" * 75)
print(
    "Retailverse Week 4/5 - Bronze Ingestion"
)
print("=" * 75)

print(
    f"Catalog:            "
    f"{context['catalog']}"
)

print(
    f"Schema:             "
    f"{context['schema']}"
)

print(
    f"Volume:             "
    f"{VOLUME_NAME}"
)

print(
    f"Source batch:       "
    f"{SOURCE_BATCH_ID}"
)

print(
    f"Source system:      "
    f"{SOURCE_SYSTEM}"
)

print(
    f"Pipeline run ID:    "
    f"{PIPELINE_RUN_ID}"
)

print(
    "Schema evolution:   "
    f"{ALLOW_ADDITIVE_SCHEMA_EVOLUTION}"
)


# COMMAND ----------
# ============================================================
# 6. VOLUME CONFIGURATION
# ============================================================

VOLUME_ROOT = (
    f"/Volumes/"
    f"{CATALOG}/"
    f"{SCHEMA}/"
    f"{VOLUME_NAME}"
)


print()
print(
    f"Volume root:\n"
    f"{VOLUME_ROOT}"
)


try:
    volume_contents = (
        dbutils.fs.ls(
            VOLUME_ROOT
        )
    )

except Exception as exc:
    raise RuntimeError(
        "Unable to access the configured "
        f"Unity Catalog Volume:\n{VOLUME_ROOT}"
    ) from exc


print()
print(
    "PASS: Unity Catalog Volume is accessible."
)


# COMMAND ----------
# ============================================================
# 7. SQL IDENTIFIER HELPERS
# ============================================================


def quote_identifier(
    value: str,
) -> str:

    validate_identifier(
        value,
        "SQL identifier",
    )

    return f"`{value}`"


def qualified_table(
    table_name: str,
) -> str:

    validate_identifier(
        table_name,
        "table",
    )

    return (
        f"{CATALOG}."
        f"{SCHEMA}."
        f"{table_name}"
    )


def quoted_qualified_table(
    table_name: str,
) -> str:

    return (
        f"{quote_identifier(CATALOG)}."
        f"{quote_identifier(SCHEMA)}."
        f"{quote_identifier(table_name)}"
    )


# COMMAND ----------
# ============================================================
# 8. EXPLICIT SOURCE SCHEMAS
# ============================================================
#
# IMPORTANT:
#
# Raw source attributes are all STRING.
#
# Bronze preserves:
#
#     "TWO"
#     "-19.99"
#     "TRUE"
#     "Y"
#     "21/06/1989"
#
# Silver will interpret them later.
#
# ============================================================


def raw_string_schema(
    *columns: str,
) -> StructType:

    return StructType(
        [
            StructField(
                column,
                StringType(),
                True,
            )
            for column in columns
        ]
    )


CUSTOMER_SCHEMA = raw_string_schema(
    "source_record_id",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "loyalty_status",
    "marketing_opt_in",
    "created_at",
    "updated_at",
)


ADDRESS_SCHEMA = raw_string_schema(
    "source_record_id",
    "address_id",
    "customer_id",
    "address_type",
    "address_line_1",
    "address_line_2",
    "city",
    "state_region",
    "postal_code",
    "country_code",
    "is_primary",
    "created_at",
    "updated_at",
)


PRODUCT_SCHEMA = raw_string_schema(
    "source_record_id",
    "product_id",
    "sku",
    "product_name",
    "brand",
    "unit_price",
    "currency_code",
    "is_active",
    "created_at",
    "updated_at",
)


CATEGORY_SCHEMA = raw_string_schema(
    "source_record_id",
    "category_id",
    "parent_category_id",
    "category_name",
    "category_code",
    "is_active",
    "created_at",
    "updated_at",
)


PRODUCT_CATEGORY_SCHEMA = raw_string_schema(
    "source_record_id",
    "product_category_id",
    "product_id",
    "category_id",
    "is_primary",
    "active_from",
    "active_to",
    "created_at",
    "updated_at",
)


ORDER_SCHEMA = raw_string_schema(
    "source_record_id",
    "order_id",
    "customer_id",
    "billing_address_id",
    "shipping_address_id",
    "order_timestamp",
    "channel",
    "order_status",
    "currency_code",
    "subtotal_amount",
    "discount_amount",
    "tax_amount",
    "shipping_amount",
    "order_total_amount",
    "created_at",
    "updated_at",
)


ORDER_ITEM_SCHEMA = raw_string_schema(
    "source_record_id",
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "tax_amount",
    "line_total_amount",
    "created_at",
    "updated_at",
)


PAYMENT_SCHEMA = raw_string_schema(
    "source_record_id",
    "payment_id",
    "order_id",
    "customer_id",
    "payment_timestamp",
    "payment_method",
    "payment_status",
    "amount",
    "currency_code",
    "transaction_reference",
    "created_at",
    "updated_at",
)


RETURN_SCHEMA = raw_string_schema(
    "source_record_id",
    "return_id",
    "order_id",
    "customer_id",
    "return_timestamp",
    "return_status",
    "return_reason",
    "total_refund_amount",
    "created_at",
    "updated_at",
)


RETURN_ITEM_SCHEMA = raw_string_schema(
    "source_record_id",
    "return_item_id",
    "return_id",
    "order_item_id",
    "product_id",
    "quantity",
    "refund_amount",
    "return_reason",
    "created_at",
    "updated_at",
)


# COMMAND ----------
# ============================================================
# 9. ENTITY CONTRACT REGISTRY
# ============================================================
#
# cast_rules are for AUDITING ONLY.
#
# They do not cast Bronze values.
#
# ============================================================

ENTITY_CONTRACTS = {
    "customer": {
        "schema": CUSTOMER_SCHEMA,
        "business_key": "customer_id",
        "bronze_table": (
            "bronze_customer"
        ),
        "cast_rules": {
            "date_of_birth": "DATE",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "address": {
        "schema": ADDRESS_SCHEMA,
        "business_key": "address_id",
        "bronze_table": (
            "bronze_customer_address"
        ),
        "cast_rules": {
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "product": {
        "schema": PRODUCT_SCHEMA,
        "business_key": "product_id",
        "bronze_table": (
            "bronze_product"
        ),
        "cast_rules": {
            "unit_price": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "category": {
        "schema": CATEGORY_SCHEMA,
        "business_key": "category_id",
        "bronze_table": (
            "bronze_category"
        ),
        "cast_rules": {
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "product_category": {
        "schema": (
            PRODUCT_CATEGORY_SCHEMA
        ),
        "business_key": (
            "product_category_id"
        ),
        "bronze_table": (
            "bronze_product_category"
        ),
        "cast_rules": {
            "active_from": "DATE",
            "active_to": "DATE",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "order": {
        "schema": ORDER_SCHEMA,
        "business_key": "order_id",
        "bronze_table": (
            "bronze_order"
        ),
        "cast_rules": {
            "order_timestamp": "TIMESTAMP",
            "subtotal_amount": "DECIMAL(18,4)",
            "discount_amount": "DECIMAL(18,4)",
            "tax_amount": "DECIMAL(18,4)",
            "shipping_amount": "DECIMAL(18,4)",
            "order_total_amount": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "order_item": {
        "schema": ORDER_ITEM_SCHEMA,
        "business_key": "order_item_id",
        "bronze_table": (
            "bronze_order_item"
        ),
        "cast_rules": {
            "quantity": "INT",
            "unit_price": "DECIMAL(18,4)",
            "discount_amount": "DECIMAL(18,4)",
            "tax_amount": "DECIMAL(18,4)",
            "line_total_amount": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "payment": {
        "schema": PAYMENT_SCHEMA,
        "business_key": "payment_id",
        "bronze_table": (
            "bronze_payment"
        ),
        "cast_rules": {
            "payment_timestamp": "TIMESTAMP",
            "amount": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "return": {
        "schema": RETURN_SCHEMA,
        "business_key": "return_id",
        "bronze_table": (
            "bronze_return"
        ),
        "cast_rules": {
            "return_timestamp": "TIMESTAMP",
            "total_refund_amount": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },

    "return_item": {
        "schema": RETURN_ITEM_SCHEMA,
        "business_key": "return_item_id",
        "bronze_table": (
            "bronze_return_item"
        ),
        "cast_rules": {
            "quantity": "INT",
            "refund_amount": "DECIMAL(18,4)",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        },
    },
}


# COMMAND ----------
# ============================================================
# 10. EXPECTED SOURCE MANIFEST COUNTS
# ============================================================
#
# These values came from our locally generated and validated
# source manifests.
#
# This lets Databricks detect incomplete/truncated uploads.
#
# ============================================================

EXPECTED_ROW_COUNTS = {
    "B001": {
        "customer": 50_108,
        "address": 64_138,
        "product": 502,
        "category": 32,
        "product_category": 760,
        "order": 120_821,
        "order_item": 371_835,
        "payment": 126_246,
        "return": 21_300,
        "return_item": 36_461,
    },

    "B002": {
        "customer": 1_703,
        "address": 1_824,
        "product": 152,
        "category": 12,
        "product_category": 113,
        "order": 10_002,
        "order_item": 30_876,
        "payment": 10_390,
        "return": 1_501,
        "return_item": 2_590,
    },
}


if SOURCE_BATCH_ID not in (
    EXPECTED_ROW_COUNTS
):
    raise ValueError(
        "No expected source manifest exists "
        f"for batch {SOURCE_BATCH_ID!r}."
    )


# COMMAND ----------
# ============================================================
# 11. SOURCE FILE DISCOVERY
# ============================================================
#
# Supports either:
#
# raw_files/
#   customer_B001.csv
#
# OR:
#
# raw_files/
#   B001/
#     customer_B001.csv
#
# This means the notebook is not tied unnecessarily to one
# folder arrangement.
#
# ============================================================


def directory_contains_file(
    directory: str,
    file_name: str,
) -> bool:

    try:
        return any(
            item.name.rstrip("/")
            == file_name
            for item
            in dbutils.fs.ls(
                directory
            )
        )

    except Exception:
        return False


def resolve_source_file(
    entity: str,
) -> str:

    file_name = (
        f"{entity}_"
        f"{SOURCE_BATCH_ID}.csv"
    )

    candidate_directories = [
        (
            f"{VOLUME_ROOT}/"
            f"{SOURCE_BATCH_ID}"
        ),
        VOLUME_ROOT,
    ]

    for directory in (
        candidate_directories
    ):
        if directory_contains_file(
            directory,
            file_name,
        ):
            return (
                f"{directory}/"
                f"{file_name}"
            )

    raise FileNotFoundError(
        "Expected source file was not "
        "found in the Unity Catalog Volume.\n"
        f"Entity: {entity}\n"
        f"File:   {file_name}\n"
        f"Volume: {VOLUME_ROOT}"
    )


def get_file_size_bytes(
    file_path: str,
) -> int:

    directory, file_name = (
        file_path.rsplit(
            "/",
            1,
        )
    )

    for item in dbutils.fs.ls(
        directory
    ):
        if (
            item.name.rstrip("/")
            == file_name
        ):
            return int(
                item.size
            )

    raise FileNotFoundError(
        file_path
    )


# COMMAND ----------
# ============================================================
# 12. VERIFY ALL TEN SOURCE FILES EXIST
# ============================================================

SOURCE_FILES = {}


for entity in (
    ENTITY_CONTRACTS
):
    SOURCE_FILES[entity] = (
        resolve_source_file(
            entity
        )
    )


print()
print(
    "Resolved source files"
)
print("-" * 75)


for entity, path in (
    SOURCE_FILES.items()
):
    print(
        f"{entity:<18} -> {path}"
    )


print()
print(
    "PASS: All 10 source files exist."
)


# COMMAND ----------
# ============================================================
# 13. READ CSV HEADER BEFORE DATA INGESTION
# ============================================================
#
# This is how we reconcile:
#
#     explicit schema enforcement
#
# with:
#
#     controlled additive schema evolution
#
#
# Spark cannot discover a new column if we blindly supply an old
# explicit schema.
#
# We therefore:
#
# 1. read ONLY the header
# 2. validate it against the contract
# 3. optionally allow new trailing STRING attributes
# 4. construct an explicit runtime StructType
# 5. read the data using that schema
#
# ============================================================


def read_csv_header(
    file_path: str,
) -> list[str]:

    header_row = (
        spark
        .read
        .text(
            file_path
        )
        .limit(1)
        .first()
    )

    if header_row is None:
        raise RuntimeError(
            f"Source file is empty: "
            f"{file_path}"
        )

    header_line = (
        header_row["value"]
    )

    columns = next(
        csv.reader(
            [header_line]
        )
    )

    # Remove UTF-8 BOM if one exists.
    if columns:
        columns[0] = (
            columns[0]
            .lstrip("\ufeff")
        )

    return columns


# COMMAND ----------
# ============================================================
# 14. HEADER / SCHEMA VALIDATION
# ============================================================


RESERVED_METADATA_COLUMNS = {
    "_source_system",
    "_source_file_name",
    "_source_file_path",
    "_source_file_size_bytes",
    "_source_batch_id",
    "_batch_id",
    "_ingested_timestamp",
    "_record_hash",
    "_corrupt_record",
}


def validate_source_header(
    entity: str,
    header_columns: list[str],
    expected_schema: StructType,
) -> list[str]:

    expected_columns = [
        field.name
        for field
        in expected_schema.fields
    ]

    base_count = len(
        expected_columns
    )

    observed_base = (
        header_columns[
            :base_count
        ]
    )

    if (
        observed_base
        != expected_columns
    ):
        raise RuntimeError(
            "Source schema contract "
            f"failed for {entity}.\n\n"
            f"Expected base columns:\n"
            f"{expected_columns}\n\n"
            f"Observed header:\n"
            f"{header_columns}"
        )

    extra_columns = (
        header_columns[
            base_count:
        ]
    )

    if extra_columns:

        if not (
            ALLOW_ADDITIVE_SCHEMA_EVOLUTION
        ):
            raise RuntimeError(
                f"{entity} contains unexpected "
                "new source columns while schema "
                "evolution is disabled:\n"
                f"{extra_columns}"
            )

        for column_name in (
            extra_columns
        ):
            validate_identifier(
                column_name,
                "new source column",
            )

    collisions = (
        set(header_columns)
        & RESERVED_METADATA_COLUMNS
    )

    if collisions:
        raise RuntimeError(
            "Source columns collide with "
            "reserved ingestion metadata:\n"
            f"{sorted(collisions)}"
        )

    return extra_columns


# COMMAND ----------
# ============================================================
# 15. BUILD RUNTIME EXPLICIT SCHEMA
# ============================================================


def build_runtime_schema(
    expected_schema: StructType,
    extra_columns: list[str],
) -> StructType:

    fields = list(
        expected_schema.fields
    )

    for column_name in (
        extra_columns
    ):
        fields.append(
            StructField(
                column_name,
                StringType(),
                True,
            )
        )

    # Special Spark CSV parser column.
    #
    # Structurally malformed records can be captured here rather
    # than disappearing invisibly.

    fields.append(
        StructField(
            "_corrupt_record",
            StringType(),
            True,
        )
    )

    return StructType(
        fields
    )


# COMMAND ----------
# ============================================================
# 16. RAW CSV READER
# ============================================================


def read_source_csv(
    entity: str,
    contract: dict,
) -> tuple[
    DataFrame,
    list[str],
]:

    file_path = (
        SOURCE_FILES[
            entity
        ]
    )

    header_columns = (
        read_csv_header(
            file_path
        )
    )

    extra_columns = (
        validate_source_header(
            entity,
            header_columns,
            contract["schema"],
        )
    )

    runtime_schema = (
        build_runtime_schema(
            contract["schema"],
            extra_columns,
        )
    )

    df = (
        spark
        .read
        .format("csv")

        # Explicit schema.
        .schema(
            runtime_schema
        )

        # Source files contain headers.
        .option(
            "header",
            "true",
        )

        # Preserve quoted/multiline text correctly.
        .option(
            "multiLine",
            "true",
        )

        .option(
            "quote",
            '"',
        )

        .option(
            "escape",
            '"',
        )

        # Structurally malformed CSV rows are captured.
        .option(
            "mode",
            "PERMISSIVE",
        )

        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record",
        )

        .option(
            "enforceSchema",
            "true",
        )

        .load(
            file_path
        )
    )

    return (
        df,
        header_columns,
    )


# COMMAND ----------
# ============================================================
# 17. PHYSICAL SOURCE RECORD VALIDATION
# ============================================================


def validate_source_record_ids(
    df: DataFrame,
) -> None:

    metrics = (
        df
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "row_count"
            ),

            F.countDistinct(
                F.col(
                    "source_record_id"
                )
            ).alias(
                "distinct_source_ids"
            ),

            F.sum(
                F.when(
                    F.col(
                        "source_record_id"
                    ).isNull(),
                    F.lit(1),
                )
                .otherwise(
                    F.lit(0)
                )
            ).alias(
                "null_source_ids"
            ),
        )
        .first()
    )

    row_count = int(
        metrics[
            "row_count"
        ]
    )

    distinct_count = int(
        metrics[
            "distinct_source_ids"
        ]
    )

    null_count = int(
        metrics[
            "null_source_ids"
        ]
        or 0
    )

    if null_count != 0:
        raise RuntimeError(
            "source_record_id contains "
            f"{null_count} NULL values."
        )

    if (
        distinct_count
        != row_count
    ):
        raise RuntimeError(
            "source_record_id must uniquely "
            "identify every physical source row.\n"
            f"Rows: {row_count}\n"
            f"Distinct IDs: {distinct_count}"
        )


# COMMAND ----------
# ============================================================
# 18. BUSINESS KEY PROFILING
# ============================================================


def profile_business_key(
    df: DataFrame,
    business_key: str,
) -> dict[str, int]:

    null_count = (
        df
        .agg(
            F.sum(
                F.when(
                    F.col(
                        business_key
                    ).isNull(),
                    F.lit(1),
                )
                .otherwise(
                    F.lit(0)
                )
            ).alias(
                "null_count"
            )
        )
        .first()[
            "null_count"
        ]
        or 0
    )

    duplicate_groups = (
        df
        .where(
            F.col(
                business_key
            ).isNotNull()
        )
        .groupBy(
            business_key
        )
        .count()
        .where(
            F.col(
                "count"
            )
            > F.lit(1)
        )
    )

    duplicate_metrics = (
        duplicate_groups
        .agg(
            F.count(
                F.lit(1)
            ).alias(
                "duplicate_key_count"
            ),

            F.sum(
                F.col(
                    "count"
                )
                - F.lit(1)
            ).alias(
                "duplicate_rows"
            ),
        )
        .first()
    )

    return {
        "null_business_key_count": int(
            null_count
        ),

        "duplicate_business_key_count": int(
            duplicate_metrics[
                "duplicate_key_count"
            ]
            or 0
        ),

        "duplicate_rows_beyond_first": int(
            duplicate_metrics[
                "duplicate_rows"
            ]
            or 0
        ),
    }


# COMMAND ----------
# ============================================================
# 19. CORRUPT CSV RECORD PROFILING
# ============================================================


def count_corrupt_records(
    df: DataFrame,
) -> int:

    return int(
        (
            df
            .agg(
                F.sum(
                    F.when(
                        F.col(
                            "_corrupt_record"
                        ).isNotNull(),
                        F.lit(1),
                    )
                    .otherwise(
                        F.lit(0)
                    )
                ).alias(
                    "corrupt_count"
                )
            )
            .first()[
                "corrupt_count"
            ]
        )
        or 0
    )


# COMMAND ----------
# ============================================================
# 20. AUDIT-ONLY DATATYPE FAILURE PROFILING
# ============================================================
#
# This NEVER modifies Bronze.
#
# Example:
#
#     quantity = "TWO"
#
# stays:
#
#     "TWO"
#
# But we record that it cannot become INT.
#
# ============================================================


def profile_invalid_casts(
    df: DataFrame,
    cast_rules: dict[str, str],
) -> tuple[
    dict[str, int],
    int,
]:

    if not cast_rules:
        return {}, 0

    aggregations = []

    for (
        column_name,
        sql_type,
    ) in cast_rules.items():

        cast_result = (
            F.expr(
                "try_cast("
                f"`{column_name}` "
                f"AS {sql_type}"
                ")"
            )
        )

        invalid_condition = (
            F.col(
                column_name
            ).isNotNull()
            & (
                F.trim(
                    F.col(
                        column_name
                    )
                )
                != F.lit("")
            )
            & cast_result.isNull()
        )

        aggregations.append(
            F.sum(
                F.when(
                    invalid_condition,
                    F.lit(1),
                )
                .otherwise(
                    F.lit(0)
                )
            )
            .cast(
                "long"
            )
            .alias(
                column_name
            )
        )

    result = (
        df
        .agg(
            *aggregations
        )
        .first()
    )

    details = {
        column_name: int(
            result[
                column_name
            ]
            or 0
        )
        for column_name
        in cast_rules
    }

    return (
        details,
        sum(
            details.values()
        ),
    )


# COMMAND ----------
# ============================================================
# 21. RECORD HASH
# ============================================================
#
# Hashes the complete raw source payload.
#
# Metadata such as _batch_id and ingestion timestamp is NOT part
# of the hash.
#
# Therefore an exact replay produces the same hash.
#
# ============================================================


def add_record_hash(
    df: DataFrame,
    raw_columns: list[str],
) -> DataFrame:

    raw_payload = (
        F.to_json(
            F.struct(
                *[
                    F.col(
                        column_name
                    )
                    .alias(
                        column_name
                    )
                    for column_name
                    in raw_columns
                ]
            ),
            options={
                "ignoreNullFields":
                    "false",
            },
        )
    )

    return (
        df
        .withColumn(
            "_record_hash",
            F.sha2(
                raw_payload,
                256,
            ),
        )
    )


# COMMAND ----------
# ============================================================
# 22. BRONZE METADATA
# ============================================================

BRONZE_METADATA_COLUMNS = [
    "_source_system",
    "_source_file_name",
    "_source_file_path",
    "_source_file_size_bytes",
    "_source_batch_id",
    "_batch_id",
    "_ingested_timestamp",
    "_record_hash",
]


def enrich_bronze(
    df: DataFrame,
    entity: str,
) -> DataFrame:

    source_path = (
        SOURCE_FILES[
            entity
        ]
    )

    file_name = (
        source_path
        .rsplit(
            "/",
            1,
        )[-1]
    )

    file_size = (
        get_file_size_bytes(
            source_path
        )
    )

    raw_columns = (
        df.columns
    )

    # Resolve one consistent ingestion timestamp for this entity.

    ingestion_timestamp = (
        spark.sql(
            """
            SELECT
                current_timestamp()
                AS ingestion_timestamp
            """
        )
        .first()[
            "ingestion_timestamp"
        ]
    )

    enriched = (
        df

        # Mandatory metadata.
        .withColumn(
            "_source_system",
            F.lit(
                SOURCE_SYSTEM
            ),
        )

        .withColumn(
            "_source_file_name",
            F.lit(
                file_name
            ),
        )

        .withColumn(
            "_batch_id",
            F.lit(
                PIPELINE_RUN_ID
            ),
        )

        .withColumn(
            "_ingested_timestamp",
            F.lit(
                ingestion_timestamp
            ).cast(
                "timestamp"
            ),
        )

        # Additional useful lineage metadata.
        .withColumn(
            "_source_file_path",
            F.lit(
                source_path
            ),
        )

        .withColumn(
            "_source_file_size_bytes",
            F.lit(
                file_size
            ).cast(
                "long"
            ),
        )

        .withColumn(
            "_source_batch_id",
            F.lit(
                SOURCE_BATCH_ID
            ),
        )
    )

    return add_record_hash(
        enriched,
        raw_columns,
    )


# COMMAND ----------
# ============================================================
# 23. BRONZE AUDIT TABLE
# ============================================================

AUDIT_TABLE_NAME = (
    "audit_bronze_ingestion"
)

AUDIT_TABLE = (
    qualified_table(
        AUDIT_TABLE_NAME
    )
)


AUDIT_SCHEMA = StructType(
    [
        StructField(
            "pipeline_run_id",
            StringType(),
            False,
        ),

        StructField(
            "source_batch_id",
            StringType(),
            False,
        ),

        StructField(
            "source_system",
            StringType(),
            False,
        ),

        StructField(
            "entity",
            StringType(),
            False,
        ),

        StructField(
            "source_file_name",
            StringType(),
            False,
        ),

        StructField(
            "source_file_path",
            StringType(),
            False,
        ),

        StructField(
            "source_file_size_bytes",
            LongType(),
            True,
        ),

        StructField(
            "target_table",
            StringType(),
            False,
        ),

        StructField(
            "source_row_count",
            LongType(),
            True,
        ),

        StructField(
            "expected_source_row_count",
            LongType(),
            True,
        ),

        StructField(
            "corrupt_record_count",
            LongType(),
            True,
        ),

        StructField(
            "null_business_key_count",
            LongType(),
            True,
        ),

        StructField(
            "duplicate_business_key_count",
            LongType(),
            True,
        ),

        StructField(
            "duplicate_rows_beyond_first",
            LongType(),
            True,
        ),

        StructField(
            "invalid_cast_failure_count",
            LongType(),
            True,
        ),

        StructField(
            "invalid_cast_details_json",
            StringType(),
            True,
        ),

        StructField(
            "replay_existing_row_count",
            LongType(),
            True,
        ),

        StructField(
            "inserted_row_count",
            LongType(),
            True,
        ),

        StructField(
            "hash_conflict_count",
            LongType(),
            True,
        ),

        StructField(
            "target_batch_row_count",
            LongType(),
            True,
        ),

        StructField(
            "target_total_row_count",
            LongType(),
            True,
        ),

        StructField(
            "status",
            StringType(),
            False,
        ),

        StructField(
            "error_message",
            StringType(),
            True,
        ),

        StructField(
            "started_at",
            TimestampType(),
            False,
        ),

        StructField(
            "completed_at",
            TimestampType(),
            False,
        ),
    ]
)


if not spark.catalog.tableExists(
    AUDIT_TABLE
):

    (
        spark
        .createDataFrame(
            [],
            AUDIT_SCHEMA,
        )
        .write
        .format(
            "delta"
        )
        .mode(
            "errorifexists"
        )
        .saveAsTable(
            AUDIT_TABLE
        )
    )

    print(
        "Created Bronze audit table:"
    )

    print(
        AUDIT_TABLE
    )


# COMMAND ----------
# ============================================================
# 24. AUDIT HELPERS
# ============================================================


def utc_now() -> datetime:

    return (
        datetime
        .now(
            timezone.utc
        )
        .replace(
            tzinfo=None
        )
    )


def write_audit_record(
    record: dict,
) -> None:

    (
        spark
        .createDataFrame(
            [record],
            AUDIT_SCHEMA,
        )
        .write
        .format(
            "delta"
        )
        .mode(
            "append"
        )
        .saveAsTable(
            AUDIT_TABLE
        )
    )


# COMMAND ----------
# ============================================================
# 25. SAFE ADDITIVE SCHEMA EVOLUTION
# ============================================================


def align_to_existing_target(
    source_df: DataFrame,
    target_table: str,
    incoming_raw_columns: list[str],
) -> DataFrame:

    target_df = (
        spark.table(
            target_table
        )
    )

    target_columns = (
        target_df.columns
    )

    missing_metadata = (
        set(
            BRONZE_METADATA_COLUMNS
        )
        - set(
            target_columns
        )
    )

    if missing_metadata:
        raise RuntimeError(
            "Existing Bronze table is "
            "missing required metadata:\n"
            f"{sorted(missing_metadata)}"
        )

    target_raw_columns = [
        column_name
        for column_name
        in target_columns
        if column_name
        not in BRONZE_METADATA_COLUMNS
    ]

    new_source_columns = [
        column_name
        for column_name
        in incoming_raw_columns
        if column_name
        not in target_raw_columns
    ]

    if new_source_columns:

        if not (
            ALLOW_ADDITIVE_SCHEMA_EVOLUTION
        ):
            raise RuntimeError(
                "New source columns detected "
                "but schema evolution is disabled:\n"
                f"{new_source_columns}"
            )

        sql_target = (
            quoted_qualified_table(
                target_table
                .split(".")[-1]
            )
        )

        for column_name in (
            new_source_columns
        ):

            validate_identifier(
                column_name,
                "new source column",
            )

            spark.sql(
                f"""
                ALTER TABLE {sql_target}
                ADD COLUMNS (
                    {quote_identifier(column_name)}
                    STRING
                )
                """
            )

            print(
                "Schema evolution:"
                f" added {column_name} STRING"
            )

    # Refresh schema after possible ALTER TABLE.

    target_df = (
        spark.table(
            target_table
        )
    )

    target_columns = (
        target_df.columns
    )

    target_raw_columns = [
        column_name
        for column_name
        in target_columns
        if column_name
        not in BRONZE_METADATA_COLUMNS
    ]

    aligned = source_df

    # Allows replaying an older source batch after a future source
    # schema has gained an optional column.

    for column_name in (
        target_raw_columns
    ):

        if (
            column_name
            not in aligned.columns
        ):
            aligned = (
                aligned
                .withColumn(
                    column_name,
                    F.lit(
                        None
                    ).cast(
                        "string"
                    ),
                )
            )

    # Bronze raw fields must always remain STRING.

    for field in (
        target_df.schema.fields
    ):

        if (
            field.name
            not in BRONZE_METADATA_COLUMNS
            and not isinstance(
                field.dataType,
                StringType,
            )
        ):
            raise RuntimeError(
                "Existing Bronze raw column "
                f"{field.name!r} has datatype "
                f"{field.dataType.simpleString()}. "
                "Raw Bronze fields must remain STRING."
            )

    return (
        aligned
        .select(
            *target_columns
        )
    )


# COMMAND ----------
# ============================================================
# 26. IDEMPOTENT BRONZE DELTA WRITE
# ============================================================
#
# RECORD IDENTITY
# ---------------
#
# source system
# +
# source batch
# +
# source_record_id
#
#
# BEHAVIOUR
# ---------
#
# First arrival:
#     INSERT
#
# Exact replay:
#     NO-OP
#
# Same physical ID but different payload:
#     FAIL
#
# New B002 source record:
#     INSERT
#
# ============================================================


def write_bronze(
    source_df: DataFrame,
    target_table: str,
) -> dict[str, int]:

    source_count = (
        source_df.count()
    )

    # --------------------------------------------------------
    # First-ever load
    # --------------------------------------------------------

    if not spark.catalog.tableExists(
        target_table
    ):

        (
            source_df
            .write
            .format(
                "delta"
            )
            .mode(
                "errorifexists"
            )
            .saveAsTable(
                target_table
            )
        )

        return {
            "replay_existing_row_count": 0,
            "inserted_row_count": (
                source_count
            ),
            "hash_conflict_count": 0,
            "target_batch_row_count": (
                source_count
            ),
            "target_total_row_count": (
                source_count
            ),
        }

    # --------------------------------------------------------
    # Existing Bronze table
    # --------------------------------------------------------

    incoming_raw_columns = [
        column_name
        for column_name
        in source_df.columns
        if column_name
        not in BRONZE_METADATA_COLUMNS
    ]

    aligned_source = (
        align_to_existing_target(
            source_df,
            target_table,
            incoming_raw_columns,
        )
    )

    # Only compare against the same source batch/system.

    existing_batch = (
        spark
        .table(
            target_table
        )
        .where(
            (
                F.col(
                    "_source_system"
                )
                == F.lit(
                    SOURCE_SYSTEM
                )
            )
            & (
                F.col(
                    "_source_batch_id"
                )
                == F.lit(
                    SOURCE_BATCH_ID
                )
            )
        )
        .select(
            "source_record_id",
            "_record_hash",
        )
    )

    before_batch_count = (
        existing_batch.count()
    )

    # --------------------------------------------------------
    # Detect exact replays vs mutated source history
    # --------------------------------------------------------

    matches = (
        aligned_source
        .select(
            "source_record_id",
            "_record_hash",
        )
        .alias(
            "incoming"
        )
        .join(
            existing_batch.alias(
                "existing"
            ),
            F.col(
                "incoming.source_record_id"
            )
            == F.col(
                "existing.source_record_id"
            ),
            "inner",
        )
    )

    match_metrics = (
        matches
        .agg(
            F.sum(
                F.when(
                    F.col(
                        "incoming._record_hash"
                    )
                    == F.col(
                        "existing._record_hash"
                    ),
                    F.lit(1),
                )
                .otherwise(
                    F.lit(0)
                )
            ).alias(
                "exact_replays"
            ),

            F.sum(
                F.when(
                    F.col(
                        "incoming._record_hash"
                    )
                    != F.col(
                        "existing._record_hash"
                    ),
                    F.lit(1),
                )
                .otherwise(
                    F.lit(0)
                )
            ).alias(
                "hash_conflicts"
            ),
        )
        .first()
    )

    exact_replays = int(
        match_metrics[
            "exact_replays"
        ]
        or 0
    )

    hash_conflicts = int(
        match_metrics[
            "hash_conflicts"
        ]
        or 0
    )

    if hash_conflicts > 0:

        raise RuntimeError(
            f"{hash_conflicts} Bronze source "
            "history conflict(s) detected.\n\n"
            "The same source system + batch + "
            "source_record_id has arrived with "
            "different raw data.\n\n"
            "Bronze history will not be silently "
            "rewritten."
        )

    # --------------------------------------------------------
    # Insert-only Delta MERGE
    # --------------------------------------------------------

    delta_target = (
        DeltaTable.forName(
            spark,
            target_table,
        )
    )

    (
        delta_target
        .alias(
            "target"
        )
        .merge(
            aligned_source.alias(
                "source"
            ),
            """
            target._source_system
                = source._source_system
            AND
            target._source_batch_id
                = source._source_batch_id
            AND
            target.source_record_id
                = source.source_record_id
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # --------------------------------------------------------
    # Post-write reconciliation
    # --------------------------------------------------------

    target_after = (
        spark.table(
            target_table
        )
    )

    after_batch_count = (
        target_after
        .where(
            (
                F.col(
                    "_source_system"
                )
                == F.lit(
                    SOURCE_SYSTEM
                )
            )
            & (
                F.col(
                    "_source_batch_id"
                )
                == F.lit(
                    SOURCE_BATCH_ID
                )
            )
        )
        .count()
    )

    total_target_count = (
        target_after.count()
    )

    inserted_count = (
        after_batch_count
        - before_batch_count
    )

    if (
        after_batch_count
        != source_count
    ):

        raise RuntimeError(
            "Bronze volume reconciliation failed.\n"
            f"Source rows:       {source_count}\n"
            f"Bronze batch rows: {after_batch_count}"
        )

    return {
        "replay_existing_row_count": (
            exact_replays
        ),

        "inserted_row_count": (
            inserted_count
        ),

        "hash_conflict_count": (
            hash_conflicts
        ),

        "target_batch_row_count": (
            after_batch_count
        ),

        "target_total_row_count": (
            total_target_count
        ),
    }


# COMMAND ----------
# ============================================================
# 27. COMPLETE SINGLE-ENTITY INGESTION
# ============================================================


def ingest_entity(
    entity: str,
    contract: dict,
) -> dict:

    started_at = (
        utc_now()
    )

    source_path = (
        SOURCE_FILES[
            entity
        ]
    )

    source_file_name = (
        source_path
        .rsplit(
            "/",
            1,
        )[-1]
    )

    source_file_size = (
        get_file_size_bytes(
            source_path
        )
    )

    target_table = (
        qualified_table(
            contract[
                "bronze_table"
            ]
        )
    )

    expected_count = int(
        EXPECTED_ROW_COUNTS[
            SOURCE_BATCH_ID
        ][entity]
    )

    source_df = None

    source_count = None
    corrupt_count = None

    null_business_keys = None

    duplicate_business_keys = None
    duplicate_rows = None

    invalid_cast_total = None
    invalid_cast_details = {}

    replay_existing = None
    inserted = None
    hash_conflicts = None

    target_batch_count = None
    target_total_count = None

    try:

        print()
        print("=" * 75)
        print(
            f"Ingesting entity: "
            f"{entity}"
        )
        print("=" * 75)

        print(
            f"Source file: "
            f"{source_path}"
        )

        print(
            f"Target:      "
            f"{target_table}"
        )

        # ----------------------------------------------------
        # A. Explicit-schema CSV read
        # ----------------------------------------------------

        (
            source_df,
            header_columns,
        ) = read_source_csv(
            entity,
            contract,
        )

        # Multiple actions will follow.
        #
        # Cache ONE entity at a time, then unpersist it.

        source_df = (
            source_df
            .persist(
                StorageLevel
                .MEMORY_AND_DISK
            )
        )

        # ----------------------------------------------------
        # B. Volumetric validation
        # ----------------------------------------------------

        source_count = (
            source_df.count()
        )

        if (
            source_count
            != expected_count
        ):

            raise RuntimeError(
                "Source row count does not "
                "match the validated source manifest.\n\n"
                "Possible causes include:\n"
                "- incomplete upload\n"
                "- truncated file\n"
                "- incorrect batch\n"
                "- unexpected source mutation\n\n"
                f"Expected: {expected_count:,}\n"
                f"Observed: {source_count:,}"
            )

        # ----------------------------------------------------
        # C. CSV structural corruption
        # ----------------------------------------------------

        corrupt_count = (
            count_corrupt_records(
                source_df
            )
        )

        if corrupt_count > 0:

            raise RuntimeError(
                f"{corrupt_count} structurally "
                "corrupt CSV record(s) detected.\n"
                "The batch will not be written "
                "because raw source fidelity "
                "cannot be guaranteed."
            )

        # ----------------------------------------------------
        # D. Physical source identity
        # ----------------------------------------------------

        validate_source_record_ids(
            source_df
        )

        # ----------------------------------------------------
        # E. Business key profiling
        # ----------------------------------------------------

        key_profile = (
            profile_business_key(
                source_df,
                contract[
                    "business_key"
                ],
            )
        )

        null_business_keys = (
            key_profile[
                "null_business_key_count"
            ]
        )

        duplicate_business_keys = (
            key_profile[
                "duplicate_business_key_count"
            ]
        )

        duplicate_rows = (
            key_profile[
                "duplicate_rows_beyond_first"
            ]
        )

        # ----------------------------------------------------
        # F. Audit-only cast checking
        # ----------------------------------------------------

        (
            invalid_cast_details,
            invalid_cast_total,
        ) = profile_invalid_casts(
            source_df,
            contract[
                "cast_rules"
            ],
        )

        # ----------------------------------------------------
        # G. Bronze metadata
        # ----------------------------------------------------

        bronze_df = (
            enrich_bronze(
                source_df,
                entity,
            )
        )

        # ----------------------------------------------------
        # H. Delta write
        # ----------------------------------------------------

        write_metrics = (
            write_bronze(
                bronze_df,
                target_table,
            )
        )

        replay_existing = (
            write_metrics[
                "replay_existing_row_count"
            ]
        )

        inserted = (
            write_metrics[
                "inserted_row_count"
            ]
        )

        hash_conflicts = (
            write_metrics[
                "hash_conflict_count"
            ]
        )

        target_batch_count = (
            write_metrics[
                "target_batch_row_count"
            ]
        )

        target_total_count = (
            write_metrics[
                "target_total_row_count"
            ]
        )

        completed_at = (
            utc_now()
        )

        # ----------------------------------------------------
        # I. Success audit
        # ----------------------------------------------------

        audit_record = {
            "pipeline_run_id": (
                PIPELINE_RUN_ID
            ),

            "source_batch_id": (
                SOURCE_BATCH_ID
            ),

            "source_system": (
                SOURCE_SYSTEM
            ),

            "entity": (
                entity
            ),

            "source_file_name": (
                source_file_name
            ),

            "source_file_path": (
                source_path
            ),

            "source_file_size_bytes": (
                source_file_size
            ),

            "target_table": (
                target_table
            ),

            "source_row_count": (
                source_count
            ),

            "expected_source_row_count": (
                expected_count
            ),

            "corrupt_record_count": (
                corrupt_count
            ),

            "null_business_key_count": (
                null_business_keys
            ),

            "duplicate_business_key_count": (
                duplicate_business_keys
            ),

            "duplicate_rows_beyond_first": (
                duplicate_rows
            ),

            "invalid_cast_failure_count": (
                invalid_cast_total
            ),

            "invalid_cast_details_json": (
                json.dumps(
                    invalid_cast_details,
                    sort_keys=True,
                )
            ),

            "replay_existing_row_count": (
                replay_existing
            ),

            "inserted_row_count": (
                inserted
            ),

            "hash_conflict_count": (
                hash_conflicts
            ),

            "target_batch_row_count": (
                target_batch_count
            ),

            "target_total_row_count": (
                target_total_count
            ),

            "status": (
                "SUCCESS"
            ),

            "error_message": (
                None
            ),

            "started_at": (
                started_at
            ),

            "completed_at": (
                completed_at
            ),
        }

        write_audit_record(
            audit_record
        )

        print()
        print(
            f"PASS: {entity}"
        )

        print(
            f"  source rows:        "
            f"{source_count:,}"
        )

        print(
            f"  inserted:           "
            f"{inserted:,}"
        )

        print(
            f"  replay matches:     "
            f"{replay_existing:,}"
        )

        print(
            f"  NULL business keys: "
            f"{null_business_keys:,}"
        )

        print(
            f"  duplicate keys:     "
            f"{duplicate_business_keys:,}"
        )

        print(
            f"  invalid casts:      "
            f"{invalid_cast_total:,}"
        )

        print(
            f"  corrupt records:    "
            f"{corrupt_count:,}"
        )

        return (
            audit_record
        )

    except Exception as exc:

        completed_at = (
            utc_now()
        )

        error_message = (
            str(exc)[
                :4000
            ]
        )

        audit_record = {
            "pipeline_run_id": (
                PIPELINE_RUN_ID
            ),

            "source_batch_id": (
                SOURCE_BATCH_ID
            ),

            "source_system": (
                SOURCE_SYSTEM
            ),

            "entity": (
                entity
            ),

            "source_file_name": (
                source_file_name
            ),

            "source_file_path": (
                source_path
            ),

            "source_file_size_bytes": (
                source_file_size
            ),

            "target_table": (
                target_table
            ),

            "source_row_count": (
                source_count
            ),

            "expected_source_row_count": (
                expected_count
            ),

            "corrupt_record_count": (
                corrupt_count
            ),

            "null_business_key_count": (
                null_business_keys
            ),

            "duplicate_business_key_count": (
                duplicate_business_keys
            ),

            "duplicate_rows_beyond_first": (
                duplicate_rows
            ),

            "invalid_cast_failure_count": (
                invalid_cast_total
            ),

            "invalid_cast_details_json": (
                json.dumps(
                    invalid_cast_details,
                    sort_keys=True,
                )
            ),

            "replay_existing_row_count": (
                replay_existing
            ),

            "inserted_row_count": (
                inserted
            ),

            "hash_conflict_count": (
                hash_conflicts
            ),

            "target_batch_row_count": (
                target_batch_count
            ),

            "target_total_row_count": (
                target_total_count
            ),

            "status": (
                "FAILED"
            ),

            "error_message": (
                error_message
            ),

            "started_at": (
                started_at
            ),

            "completed_at": (
                completed_at
            ),
        }

        write_audit_record(
            audit_record
        )

        raise

    finally:

        if source_df is not None:
            source_df.unpersist()


# COMMAND ----------
# ============================================================
# 28. RUN ALL TEN ENTITIES
# ============================================================

RUN_RESULTS = []


for (
    entity,
    contract,
) in ENTITY_CONTRACTS.items():

    result = (
        ingest_entity(
            entity,
            contract,
        )
    )

    RUN_RESULTS.append(
        result
    )


# COMMAND ----------
# ============================================================
# 29. PIPELINE RUN SUMMARY
# ============================================================

run_summary_df = (
    spark
    .table(
        AUDIT_TABLE
    )
    .where(
        F.col(
            "pipeline_run_id"
        )
        == F.lit(
            PIPELINE_RUN_ID
        )
    )
    .select(
        "entity",
        "status",
        "source_file_name",
        "source_row_count",
        "expected_source_row_count",
        "corrupt_record_count",
        "null_business_key_count",
        "duplicate_business_key_count",
        "duplicate_rows_beyond_first",
        "invalid_cast_failure_count",
        "replay_existing_row_count",
        "inserted_row_count",
        "hash_conflict_count",
        "target_batch_row_count",
        "target_total_row_count",
    )
    .orderBy(
        "entity"
    )
)


display(
    run_summary_df
)


# COMMAND ----------
# ============================================================
# 30. FINAL PIPELINE ASSERTIONS
# ============================================================

summary = (
    run_summary_df
    .agg(
        F.count(
            F.lit(1)
        ).alias(
            "entity_count"
        ),

        F.sum(
            F.when(
                F.col(
                    "status"
                )
                == F.lit(
                    "SUCCESS"
                ),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            )
        ).alias(
            "success_count"
        ),

        F.sum(
            F.col(
                "hash_conflict_count"
            )
        ).alias(
            "hash_conflicts"
        ),

        F.sum(
            F.col(
                "corrupt_record_count"
            )
        ).alias(
            "corrupt_records"
        ),
    )
    .first()
)


entity_count = int(
    summary[
        "entity_count"
    ]
)

success_count = int(
    summary[
        "success_count"
    ]
)

hash_conflicts = int(
    summary[
        "hash_conflicts"
    ]
    or 0
)

corrupt_records = int(
    summary[
        "corrupt_records"
    ]
    or 0
)


if entity_count != 10:

    raise RuntimeError(
        f"Expected 10 entities, "
        f"observed {entity_count}."
    )


if success_count != 10:

    raise RuntimeError(
        f"Expected 10 successful "
        f"entities, observed "
        f"{success_count}."
    )


if hash_conflicts != 0:

    raise RuntimeError(
        "Bronze hash conflicts "
        "were detected."
    )


if corrupt_records != 0:

    raise RuntimeError(
        "Structurally corrupt CSV "
        "records were detected."
    )


print()
print("=" * 75)
print(
    "BRONZE INGESTION PASSED"
)
print("=" * 75)

print(
    f"Source batch:    "
    f"{SOURCE_BATCH_ID}"
)

print(
    f"Pipeline run ID: "
    f"{PIPELINE_RUN_ID}"
)

print()
print(
    "All 10 OLTP files were:"
)

print(
    "  ✓ located in the Unity Catalog Volume"
)

print(
    "  ✓ validated against explicit schemas"
)

print(
    "  ✓ reconciled against expected source counts"
)

print(
    "  ✓ checked for structural corruption"
)

print(
    "  ✓ profiled for NULL and duplicate business keys"
)

print(
    "  ✓ profiled for datatype interpretation failures"
)

print(
    "  ✓ enriched with lineage metadata"
)

print(
    "  ✓ written idempotently to Bronze Delta"
)