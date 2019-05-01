import os, sys
from py4j.java_gateway import JavaGateway 
from datetime import date, datetime
from time import time 

# Load jvm connecting instance
jdbc_path = './target/SqreamJDBC.jar'
gateway = JavaGateway.launch_gateway(classpath=jdbc_path)

# Load SQream JDBC Driver
gateway.jvm.Class.forName("com.sqream.jdbc.SQDriver")

# Open a connection
jdbc_uri = 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream' 
# 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=bobo'
conn = gateway.jvm.java.sql.DriverManager.getConnection(jdbc_uri, "sqream", "sqream")

       
# Create table
sql = "create or replace table perf (bools bool, bytes tinyint, shorts smallint, ints int, bigints bigint, floats real, doubles double, strangs nvarchar(10))" #, dates date, dts datetime)"
stmt = conn.createStatement()
stmt.execute(sql)
stmt.close()

# Network insert 10 million rows
amount = 10**7
start = time()
sql = "insert into perf values (?, ?, ?, ?, ?, ?, ?, ?)"
ps = conn.prepareStatement(sql)
for _ in range(amount):
    ps.setBoolean(1, True)
    ps.setByte(2, 120)
    ps.setShort(3, 1400)
    ps.setInt(4, 140000)
    ps.setLong(5, 5)
    ps.setFloat(6, 56.0)
    ps.setDouble(7, 57.0)
    ps.setString(8, "bla")
    # ps.setDate(9, date(2019, 11, 26))
    # ps.setTimestamp(10, datetime(2019, 11, 26, 16, 45, 23, 45))
    ps.addBatch()

ps.executeBatch()  
ps.close()

print ("total network insert: ",  (time() -start))

# Check amount inserted
sql = "select count(*) from perf"
stmt = conn.createStatement()
rs = stmt.executeQuery(sql)
while rs.next():
    print("row count: ", rs.getLong(1))
rs.close()
stmt.close()