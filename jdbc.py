import os, sys
from py4j.java_gateway import JavaGateway 

# Load jvm connecting instance
jdbc_path = './target/sqream-jdbc-2.9.4-jar-with-dependencies.jar'
gateway = JavaGateway.launch_gateway(classpath=jdbc_path)

# Load SQream JDBC Driver
gateway.jvm.Class.forName("com.sqream.jdbc.SQDriver")

# Open a connection
jdbc_uri = 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream' 
# 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=bobo'
conn = gateway.jvm.java.sql.DriverManager.getConnection(jdbc_uri, "sqream", "sqream")

# Execute a statement
stmt = conn.createStatement()
rs = stmt.executeQuery('select * from t_int')
while rs.next():
    print ("result:", rs.getInt(1))
rs.close()
stmt.close()
