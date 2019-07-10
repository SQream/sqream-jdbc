javac -cp .:../target/sqream-jdbc-2.9.7-jar-with-dependencies.jar:./* NetworkInsertTool.java && java -cp .:../target/sqream-jdbc-2.9.7-jar-with-dependencies.jar:./* NetworkInsertTool

Example:
javac -cp .:../target/sqream-jdbc-2.9.7-jar-with-dependencies.jar:./* NetworkInsertTool.java && java -cp .:../target/sqream-jdbc-2.9.7-jar-with-dependencies.jar:./* NetworkInsertTool -d master -i 127.0.0.1 -port 5000 -u sqream -pw sqream -del , -t t_a -csv t_a.csv