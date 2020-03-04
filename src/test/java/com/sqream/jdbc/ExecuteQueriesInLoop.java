package com.sqream.jdbc;

import org.junit.Test;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.sqream.jdbc.TestEnvironment.*;

public class ExecuteQueriesInLoop {
    private static final int THREADS = 75;
    private static final long TIME = TimeUnit.HOURS.toMillis(24);
    private static final int SLEEP_SEC = 30;


    private static final List<String> queries = new ArrayList<>();

    private boolean stopFlag = false;

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
        System.out.println("All queries are valid. Start test.");

        startTimer();

        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);
        while (!stopFlag) {
            generateQueries(THREADS).forEach((query) -> {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        executeQuery(getRandomQuery());
                    }
                });
            });
            System.out.println("Sleep 30 sec.");
            Thread.sleep(SLEEP_SEC * 1_000);
            System.out.println("Continue");
        }

        executorService.shutdown();
        executorService.awaitTermination(20, TimeUnit.MINUTES);
        System.out.println("SUCCESS");
    }

    private void startTimer() {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                stopFlag = true;
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, TIME);
    }

    private String getRandomQuery() {
        Random rand = new Random();
        return queries.get(rand.nextInt(queries.size()));
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
