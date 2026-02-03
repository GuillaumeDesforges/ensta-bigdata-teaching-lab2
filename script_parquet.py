import json
import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import datetime

# Use same schema structure as the Avro example
schema = pa.schema([
    ('date', pa.string()),
    ('temperature', pa.float64()),
])

# Same test data as Avro example
np.random.seed(42)
temperatures = 15 + np.random.randn(10000)
start_date = datetime.datetime(2021, 1, 1, 0, 0, 0)
data = [
    {
        "date": (start_date + datetime.timedelta(minutes=i)).isoformat(),
        "temperature": temperature,
    }
    for i, temperature in enumerate(temperatures)
]

# Convert to Arrow table
table = pa.Table.from_pylist(data, schema=schema)

# Write to Parquet file
filepath = pathlib.Path("tmp/data/out/temperatures.parquet")
filepath.parent.mkdir(parents=True, exist_ok=True)
pq.write_table(table, filepath, compression=None)

# Read and print the data back
table_read = pq.read_table(filepath)
for record in table_read.to_pylist():
    print(json.dumps(record, indent=2))
