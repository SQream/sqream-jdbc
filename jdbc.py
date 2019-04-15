import os, sys
import py4j
from py4j.java_gateway import JavaGateway 

jdbc_path = './target/sqream-jdbc-2.9.4-jar-with-dependencies.jar'
gateway = JavaGateway.launch_gateway(classpath=jdbc_path)
jvm = gateway.jvm
jvm.Class.forName("com.sqream.jdbc.SQDriver")

jdbc_uri = 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream' 
# 'jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false;service=bobo'

conn = jvm.java.sql.DriverManager.getConnection(jdbc_uri, "sqream", "sqream")

stmt = conn.createStatement()
rs = stmt.executeQuery('select * from t_int')
while rs.next():
    print ("result:", rs.getInt(1))
rs.close()
stmt.close()
