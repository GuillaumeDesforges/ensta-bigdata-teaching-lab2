import json
import pathlib
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema_dict = {
    "type": "record",
    "name": "EventRecord",
    "fields": [
        {
            "name": "event",
            "type": {
                "type": "record",
                "name": "Event",
                "fields": [
                    {"name": "type", "type": "string"},
                    {"name": "date", "type": "string"},
                    {"name": "path", "type": "string"},
                    {"name": "selector", "type": ["null", "string"], "default": None},
                ],
            },
        },
        {
            "name": "user",
            "type": {
                "type": "record",
                "name": "User",
                "fields": [
                    {"name": "email", "type": "string"},
                    {"name": "role", "type": "string"},
                ],
            },
        },
    ],
}
schema_json = json.dumps(schema_dict)
schema = avro.schema.parse(schema_json)

data = [
    {
        "event": {"type": "PageView", "date": "2024-02-02T11:43:14", "path": "/"},
        "user": {"email": "guillaume.desforges.pro@gmail.com", "role": "Admin"},
    },
    {
        "event": {"type": "PageView", "date": "2024-02-02T11:43:33", "path": "/blog"},
        "user": {"email": "guillaume.desforges.pro@gmail.com", "role": "Admin"},
    },
    {
        "event": {
            "type": "ElementClick",
            "date": "2024-02-02T11:56:01",
            "path": "/blog",
            "selector": "#subscribe",
        },
        "user": {"email": "guillaume.desforges.pro@gmail.com", "role": "Admin"},
    },
]

filepath = pathlib.Path("tmp/data/out/events.avro")
filepath.parent.mkdir(parents=True, exist_ok=True)
with open(filepath, "wb") as f:
    fwriter = DataFileWriter(f, DatumWriter(), schema)
    for record in data:
        fwriter.append(record)
    fwriter.close()

with open(filepath, "rb") as f:
    reader = DataFileReader(f, DatumReader())
    for record in reader:
        print(json.dumps(record, indent=2))
    reader.close()
