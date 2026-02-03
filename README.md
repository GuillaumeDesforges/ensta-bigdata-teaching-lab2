# Lab 2: Data Serialization Formats

## Setup

Create a virtual environment and install dependencies.

```bash
uv sync
```

## Avro

Apache Avro is a row-oriented data serialization format that uses JSON for defining schemas.

Run the Avro example:

```bash
uv run python script_avro.py
```

This script demonstrates:
- Defining a nested Avro schema (records with nested records)
- Writing records to an Avro file
- Reading records back from the file

> Exercise 1: Modify the schema to add a new field `timestamp` (as a long) to the `Event` record. Update the data accordingly and verify the output.

> Exercise 2: What happens if you try to read an Avro file with a different schema than the one used to write it? Experiment with adding/removing fields.

## Parquet

Apache Parquet is a columnar storage format optimized for analytics workloads.

Run the Parquet example:

```bash
uv run python script_parquet.py
```

This script demonstrates:
- Defining a schema with PyArrow
- Converting Python data to an Arrow table
- Writing to a Parquet file
- Reading data back from the file

> Exercise 3: Modify the script to write the Parquet file with `compression='snappy'`. Compare the file sizes with and without compression.

> Exercise 4: Use `pq.read_table()` with the `columns` parameter to read only the `temperature` column. What is the advantage of columnar storage for this operation?

## Comparison

> Exercise 5: For the same dataset, compare:
> - File size in Avro vs Parquet (with and without compression)
> - Read performance when accessing all columns vs a single column
