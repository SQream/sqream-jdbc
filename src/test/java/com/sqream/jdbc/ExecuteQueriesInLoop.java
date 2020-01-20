package com.sqream.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.sqream.jdbc.TestEnvironment.*;

public class ExecuteQueriesInLoop {
    private static final Logger LOGGER = Logger.getLogger(ExecuteQueriesInLoop.class.getName());

    private static final int STATEMENTS_AMOUNT = 75;
    private static final int TIMES = 100;

    private static final List<String> queries = new ArrayList<>();

    static {
        queries.add("create or replace table customers (ID integer not NULL, Fname varchar(30) not NULL,Lname nvarchar(60));");
        queries.add("select * from customers;");
        queries.add("insert into customers(ID, Fname, Lname) values (1,'Lea','Huntington');");
        queries.add("insert into customers(ID, Fname, Lname) values (1,'Lea','אאאא');");
        queries.add("insert into customers(ID, Fname, Lname) values (2,'Lea','גגגג');");
        queries.add("insert into customers(ID, Fname, Lname) values (3,'Lea','בבבב');");
        queries.add("truncate table customers;");
        queries.add("select show_server_status();");
        queries.add("select getdate();");
        queries.add("select show_server_status();");
        queries.add("insert into customers(ID, Fname, Lname) values (2,'George','Li');");
        queries.add("insert into customers(ID, Fname, Lname) values (3,'Leo','BrIDge');");
        queries.add("insert into customers(ID, Fname, Lname) values (4,'Tao','Chung');");
        queries.add("select * from sqream_catalog.tables;");
        queries.add("delete from customers where ID = 1;");
        queries.add("delete from customers where ID = 1;");
        queries.add("delete from customers where ID = 1;");
        queries.add("delete from customers where ID = 1;");
        queries.add("select CLEANUP_CHUNKS ('public','customers');");
        queries.add("SELECT CLEANUP_EXTENTS ('public','customers');");
        queries.add("select show_server_status();");
        queries.add("insert into customers select * from customers;");
        queries.add("insert into customers(ID,Fname) values (5,'Oliver Lee Chong');");
        queries.add("COPY customers TO '/home/alexk/projects/jdbc-driver/customers_copy_to.csv';");
        queries.add("COPY customers from '/home/alexk/projects/jdbc-driver/customers.csv';");
        queries.add("Create or replace external table ext_customers (ID integer not NULL, Fname varchar(20) not NULL,Lname nvarchar(20)) USING FORMAT CSV WITH PATH '/home/alexk/projects/jdbc-driver/customers.csv' field delimiter ',';");
        queries.add("select * from ext_customers;");
        queries.add("select * from ext_customers where ID > 2;");
        queries.add("select distinct Lname from customers;");
        queries.add("select * from customers order by Lname;");
        queries.add("select count (ID), Lname from customers group by Lname;");
        queries.add("select distinct Lname from ext_customers;");
        queries.add("select * from ext_customers order by Lname;");
        queries.add("select count (ID), Lname from ext_customers group by Lname;");
        queries.add("select count(ID), max(ID), sum(ID), min(ID) from customers;");
        queries.add("select count(ID), max(ID), sum(ID), min(ID) from ext_customers;");
        queries.add("select top 1 * from customers a join customers b on a.ID = b.ID;");
        queries.add("select top 10 * from customers a join customers b on a.ID = b.ID;");
    }

    @Test
    public void executeQueriesInLoop() throws InterruptedException {
        queries.forEach(this::executeQuery);

        ExecutorService executorService = Executors.newFixedThreadPool(STATEMENTS_AMOUNT);
        for (int i = 0; i < TIMES; i++) {
            generateQueries(STATEMENTS_AMOUNT).forEach((query) -> {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        executeQuery(query);
                    }
                });
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(20, TimeUnit.MINUTES);
    }

    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASS);
    }

    private void executeQuery(String query) {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {

            System.out.println(MessageFormat.format("Execute query: [{0}]", query));
            stmt.execute(query);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<String> generateQueries(int amount) {
        List<String> result = new ArrayList<>(amount);
        Random randomGenerator = new Random();
        for (int i = 0; i < amount; i++) {
            result.add(queries.get(randomGenerator.nextInt(queries.size())));
        }
        return result;
    }
}
