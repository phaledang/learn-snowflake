import pandas as pd
from datetime import datetime

df = pd.DataFrame({
    "metric": ["cpu", "memory", "disk"],
    "value":  [0.75, 0.60,   120.0],
    "ts":     [datetime(2024,1,1,9), datetime(2024,1,1,10), datetime(2024,1,1,11)]
})

df.to_parquet("sample.parquet", index=False)
print("Wrote sample.parquet")
