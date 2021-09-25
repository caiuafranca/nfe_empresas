from pyhive import hive
conn = hive.Connection(host='192.168.0.18', port=10000)

cursor = conn.cursor()

import pandas as pd
df = pd.read_sql("SELECT * FROM locacao", conn)

print(df.head(10))
