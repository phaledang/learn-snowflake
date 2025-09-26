import pyorc
from datetime import datetime

rows = [
    (1, "Alice", 30, datetime(2024,1,1,9,0,0)),
    (2, "Bob",   25, datetime(2024,1,1,9,5,0)),
    (3, "Charlie",35, datetime(2024,1,1,9,10,0)),
]

schema = "struct<id:int,name:string,age:int,ts:timestamp>"

with open("sample.orc", "wb") as f:
    with pyorc.Writer(f, schema) as writer:
        for r in rows:
            writer.write(r)

print("Wrote sample.orc")
