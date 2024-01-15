import psycopg2
from psycopg2 import pool
from kafka import KafkaConsumer 
from json import loads

host = "<your-hostname>"
dbname = "citus"
user = "citus"
password = "<your-database-password>"
sslmode = "require"

# Build a connection string from the variables
conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20,conn_string)
if (postgreSQL_pool):
    print("Connection pool created successfully")

# Use getconn() to get a connection from the connection pool
conn = postgreSQL_pool.getconn()

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS Formula_1_Race;")
print("Finished dropping table (if existed)")

cursor.execute("CREATE TABLE Formula_1_Race (Position text,Number integer,Driver text, Car text, Laps integer,Time text, Points integer,year integer,Grand_Prix text);")
print("Finished creating table")

consumer = KafkaConsumer(
    "users_created",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id = "consumer",
    consumer_timeout_ms=10000,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

for c in consumer:
    dict_value = c.value
    cursor.execute("INSERT INTO Formula_1_Race (Position,Number,Driver,Car,Laps,Time,Points,year,Grand_Prix) VALUES (%s, %s, %s, %s,%s,%s,%s,%s,%s);", (dict_value["Pos"],dict_value["No"],dict_value["Driver"],dict_value["Car"],dict_value["Laps"],dict_value["Time/Retired"],dict_value["PTS"],dict_value["Year"],dict_value["Grand Prix"]))

    
cursor.execute("SELECT * FROM Formula_1_Race;")
rows = cursor.fetchall()

# Print all rows
for row in rows:
    print(row)


# Clean up
conn.commit()
cursor.close()
conn.close()
