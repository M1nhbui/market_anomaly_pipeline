# =============================================================================
# Glue Data Catalog
#
# The Catalog is a metadata store, not a data store. It records "there is a table
# called clean_bars, its columns are these, its files live at this S3 path, in this
# format." Athena reads that description and then reads your Parquet directly from
# S3. Nothing is copied, loaded, or ingested - the table is a description of files
# that already exist.
#
# This is why the same Parquet is simultaneously readable by Athena, Spark, and
# anything else that speaks the Catalog: there is no proprietary storage layer.
# =============================================================================

locals {
  # Glue database names allow only lowercase letters, digits, and underscores.
  # "crypto-anomaly" would be rejected.
  db_prefix = replace(var.project_name, "-", "_")

  # Column definitions for Silver, EXCLUDING the partition keys.
  #
  # symbol and date are deliberately absent: partition columns are declared
  # separately in partition_keys below, and listing a column in both places is a
  # hard error. This trips almost everyone once - the mental model is that partition
  # values live in the S3 *path*, not in the Parquet files, so they are metadata
  # about where a file sits rather than data inside it.
  silver_columns = [
    { name = "open_time", type = "timestamp", comment = "Bar open, UTC. Dedup key with symbol." },
    { name = "close_time", type = "timestamp", comment = "Bar close, UTC. Used to detect unfinished bars." },
    { name = "open", type = "double", comment = "Cast from Binance string." },
    { name = "high", type = "double", comment = "Cast from Binance string." },
    { name = "low", type = "double", comment = "Cast from Binance string." },
    { name = "close", type = "double", comment = "Cast from Binance string." },
    { name = "volume", type = "double", comment = "Base asset volume." },
    { name = "quote_volume", type = "double", comment = "Quote asset (USDT) volume." },
    { name = "num_trades", type = "int", comment = "Trade count. 0 indicates an untraded minute." },
    { name = "taker_buy_base", type = "double", comment = "Taker buy base volume." },

    # Derived features (slice 4). All nullable by design - see the comments in
    # add_derived_features(). NULL here means "not computable", never "zero".
    { name = "log_return", type = "double", comment = "ln(close/prev_close). NULL on a symbol's first bar." },
    { name = "rolling_vol", type = "double", comment = "stddev_samp(log_return) over a trailing 20-MINUTE window. NULL until 20 return observations exist." },
    { name = "dollar_volume", type = "double", comment = "close * volume. Approximate - prefer quote_volume, which is Binance's exact USDT turnover." },
    { name = "range_pct", type = "double", comment = "(high-low)/open. NULL if open <= 0." },

    { name = "ingested_at", type = "timestamp", comment = "When this version was fetched. Dedup tie-break." },
    { name = "source_host", type = "string", comment = "Lineage: which Binance host served it." },
  ]
}

locals {
  # anomaly_events. Partition key `date` is declared separately, as always.
  gold_anomaly_columns = [
    { name = "symbol", type = "string", comment = "Trading pair." },
    { name = "event_time", type = "timestamp", comment = "open_time of the flagged bar, UTC." },
    { name = "rule", type = "string", comment = "return_zscore | volume_zscore" },
    { name = "rule_direction", type = "string", comment = "both (abs(z) > k) | upper (z > k). Returns are two-sided - a crash is as anomalous as a spike. Volume is upper-tail only: the lower tail was flagging untraded minutes as 'anomalously low volume', which is illiquidity, not an event." },
    { name = "observed_value", type = "double", comment = "The value actually scored - see observed_transform." },
    { name = "observed_transform", type = "string", comment = "none (log_return) | log1p (volume). Volume is log-transformed because a z-score on raw, right-skewed, non-negative volume puts its lower bound below zero." },
    { name = "rolling_mean", type = "double", comment = "Baseline mean over the trailing window, EXCLUDING this bar." },
    { name = "rolling_std", type = "double", comment = "Baseline sample stddev over the same window." },
    { name = "zscore", type = "double", comment = "(observed - rolling_mean) / rolling_std. Signed: negative means an unusually large DOWN move." },
    { name = "close_price", type = "double", comment = "Price context at the event." },
    { name = "volume", type = "double", comment = "RAW volume for context, untransformed." },
    { name = "num_trades", type = "int", comment = "Trade count context. 0 indicates an untraded minute." },
    { name = "zscore_window_bars", type = "int", comment = "Window length that produced this row." },
    { name = "zscore_threshold", type = "double", comment = "k that produced this row. Stored per-row so events from different parameter choices stay distinguishable." },
  ]
}

resource "aws_glue_catalog_database" "gold" {
  name        = "${local.db_prefix}_gold"
  description = "Analytics tables and the anomaly signal."
}

resource "aws_glue_catalog_table" "anomaly_events" {
  name          = "anomaly_events"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"

    # Projected on `date` only - no `symbol`.
    #
    # anomaly_events is SPARSE: a few hundred rows a day across all 17 symbols,
    # versus Silver's ~24,000. Partitioning by symbol as well would create 17 files
    # a day of a few KB each, and per-file overhead would exceed anything pruning
    # could save. Partition granularity should follow row counts, not habit.
    "projection.enabled"            = "true"
    "projection.date.type"          = "date"
    "projection.date.range"         = "${var.projection_start_date},NOW"
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"

    "storage.location.template" = "s3://${aws_s3_bucket.layer["gold"].id}/anomaly_events/date=$${date}"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.layer["gold"].id}/anomaly_events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "parquet.column.index.access" = "false"
      }
    }

    dynamic "columns" {
      for_each = local.gold_anomaly_columns

      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }

  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Event date (UTC). Reserved word in Athena SQL - quote it."
  }
}

# -----------------------------------------------------------------------------
# DQ run history, as a queryable table.
#
# The gate writes one JSON line PER CHECK rather than one nested object per run.
# Flat means "what is the pass rate per check over the last N runs" is a single
# GROUP BY instead of an UNNEST or a scripting exercise - and the whole reason the
# results are persisted at all is that a boolean return value makes that question
# unanswerable a day later.
#
# JSON rather than Parquet because the writer is a Lambda emitting a handful of rows
# per run. Converting to Parquet would mean a Glue job to process kilobytes.
# -----------------------------------------------------------------------------

resource "aws_glue_catalog_table" "dq_results" {
  name          = "dq_results"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "json"

    "projection.enabled"            = "true"
    "projection.date.type"          = "date"
    "projection.date.range"         = "${var.projection_start_date},NOW"
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"

    "storage.location.template" = "s3://${aws_s3_bucket.layer["gold"].id}/dq_results/date=$${date}"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.layer["gold"].id}/dq_results/"

    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        # Tolerate a field appearing in the JSON that is not in this schema, rather
        # than failing the whole query. A quality gate whose own history table breaks
        # when a check is added would be an unfortunate irony.
        "ignore.malformed.json" = "true"
        "dots.in.keys"          = "false"
      }
    }

    columns {
      name    = "run_id"
      type    = "string"
      comment = "Lambda request id - correlates a row back to CloudWatch logs."
    }
    columns {
      name    = "run_at"
      type    = "string"
      comment = "ISO-8601 UTC. Kept as string: the JSON SerDe's timestamp handling is fussy, and parsing on read with from_iso8601_timestamp() is explicit and reliable."
    }
    columns {
      name    = "check_name"
      type    = "string"
      comment = "Named check_name, not check - CHECK is a reserved word in Athena."
    }
    columns {
      name    = "severity"
      type    = "string"
      comment = "fail (blocks the pipeline) | warn (recorded and alerted, does not block)."
    }
    columns {
      name    = "passed"
      type    = "boolean"
    }
    columns {
      name    = "observed"
      type    = "double"
      comment = "The measured value. NULL when not measurable - e.g. freshness on an empty Silver layer, where the raw value is infinite and infinity is not valid JSON."
    }
    columns {
      name    = "threshold"
      type    = "double"
    }
    columns {
      name    = "detail"
      type    = "string"
      comment = "Human-readable explanation of what the check protects against."
    }
  }

  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Run date (UTC). Reserved word in Athena SQL - quote it."
  }
}

resource "aws_glue_catalog_database" "silver" {
  name        = "${local.db_prefix}_silver"
  description = "Cleaned, typed, deduplicated market bars."
}

resource "aws_glue_catalog_table" "clean_bars" {
  name          = "clean_bars"
  database_name = aws_glue_catalog_database.silver.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL              = "TRUE"
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"

    # -------------------------------------------------------------------------
    # PARTITION PROJECTION
    #
    # Normally Athena keeps a LIST of partitions in the Catalog, and a new
    # partition is invisible until you register it - via MSCK REPAIR TABLE, a Glue
    # crawler, or an explicit ALTER TABLE ADD PARTITION. That is an entire extra
    # moving part that has to run after every write, and when it does not run, your
    # freshly written data silently does not exist as far as SQL is concerned.
    #
    # Partition projection removes that step. Instead of storing a list, Athena
    # COMPUTES the possible partition values from the rules below, and derives the
    # S3 path directly. A partition written thirty seconds ago is queryable
    # immediately, with no crawler, no repair, and no cost.
    #
    # The trade-off: the rules must be right, and they must be maintained. If a
    # symbol is missing from the enum, its data is invisible - not an error, just
    # absent. That is the failure mode to watch for, and it is why the enum is fed
    # from config/symbols.json rather than typed by hand here.
    # -------------------------------------------------------------------------
    "projection.enabled" = "true"

    # Enum, because the symbol set is small, known, and version-controlled.
    "projection.symbol.type"   = "enum"
    "projection.symbol.values" = join(",", local.active_symbols)

    # Date range ending at NOW, so tomorrow's partition is projected without any
    # change here. The start date is a variable: setting it too early makes Athena
    # consider (and attempt to read) paths that never existed, which costs query
    # planning time for nothing.
    "projection.date.type"          = "date"
    "projection.date.range"         = "${var.projection_start_date},NOW"
    "projection.date.format"        = "yyyy-MM-dd"
    "projection.date.interval"      = "1"
    "projection.date.interval.unit" = "DAYS"

    # How Athena builds the S3 path from projected values.
    #
    # NOTE THE DOUBLE DOLLAR SIGNS. Terraform interpolates $${...}, so $${symbol}
    # emits the literal ${symbol} that Athena needs. Writing ${symbol} here would
    # make Terraform look for a variable named "symbol" and fail at plan time - a
    # confusing error given the string is meant for a different templating engine
    # entirely.
    "storage.location.template" = "s3://${aws_s3_bucket.layer["silver"].id}/symbol=$${symbol}/date=$${date}"
  }

  storage_descriptor {
    location = "s3://${aws_s3_bucket.layer["silver"].id}/"

    # Hive-era class names. They look archaic because they are - Athena inherited
    # the Hive metastore format. Copy them exactly; there is nothing to tune.
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        # Match Parquet fields by NAME rather than by position. Name matching means
        # adding a column to the Spark job does not silently shift every subsequent
        # column's data by one. This is the default, but it is worth being explicit
        # about because the failure mode - correct-looking queries returning data
        # from the wrong column - is genuinely hard to spot.
        "parquet.column.index.access" = "false"
      }
    }

    dynamic "columns" {
      for_each = local.silver_columns

      content {
        name    = columns.value.name
        type    = columns.value.type
        comment = columns.value.comment
      }
    }
  }

  # Partition keys. Their VALUES come from the S3 path, not from inside the files.
  #
  # `date` is declared as string, not date. Spark writes it as a formatted string,
  # and Hive-style partition values are strings on disk regardless. The
  # projection.date.type = "date" setting above is what tells Athena how to
  # GENERATE the values; this declares how the column is typed when read.
  partition_keys {
    name    = "symbol"
    type    = "string"
    comment = "Trading pair. Projected from the enum in config/symbols.json."
  }

  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Bar date (UTC), yyyy-MM-dd. NOTE: reserved word in Athena SQL - quote it."
  }
}
