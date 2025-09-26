from fastavro import writer, parse_schema

schema = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "event", "type": "string"},
        {"name": "user", "type": "string"},
        {"name": "amount", "type": ["null", "float"], "default": None}
    ]
}
records = [
    {"id": 1, "event": "login",    "user": "alice",   "amount": None},
    {"id": 2, "event": "purchase", "user": "bob",     "amount": 50.5},
    {"id": 3, "event": "logout",   "user": "charlie", "amount": None},
]

parsed = parse_schema(schema)
with open("sample.avro", "wb") as out:
    writer(out, parsed, records)

print("Wrote sample.avro")
