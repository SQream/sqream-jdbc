import java.sql.DriverManager
import java.sql.Connection

// :require "target/sqream-jdbc-2.9.2-jar-with-dependencies.jar"

val driver = "com.sqream.jdbc.SQDriver"
val url = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream;cluster=false;ssl=false"
val username = "sqream"
val password = "sqream"
Class.forName(driver)
val connection = DriverManager.getConnection(url, username, password)

def select(query: String) {

  val stmt = connection.createStatement()
  val rs = stmt.executeQuery(query)
  while (rs.next()) 
    println("result - " + rs.getInt(1))
  
  rs.close()
  stmt.close()
  connection.close()
}


select("select 1")