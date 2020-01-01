package com.sqream.jdbc;

import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.MessageFormat;

public class JDBC_Csv_Perf {

    private static final String SQL_CREATE_TABLE = "create or replace table lineitem (\n" +
            "  l_orderkey bigint not null ,\n" +
            "  l_partkey bigint not null ,\n" +
            "  l_suppkey int not null ,\n" +
            "  l_linenumber int not null ,\n" +
            "  l_quantity int not null ,\n" +
            "  l_extendedprice int not null ,\n" +
            "  l_discount int not null ,\n" +
            "  l_tax int not null ,\n" +
            "  l_returnflag varchar(1) not null ,\n" +
            "  l_linestatus varchar(1) not null ,\n" +
            "  l_shipdate date not null ,\n" +
            "  l_commitdate date not null ,\n" +
            "  l_receiptdate date not null ,\n" +
            "  l_shipinstruct varchar(17) not null ,\n" +
            "  l_shipmode varchar(7) not null ,\n" +
            "  l_comment nvarchar(44) not null\n" +
            " \n" +
            ")\n" +
            ";";
    private static final String SQL_INSERT = "insert into lineitem values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String URI = "jdbc:Sqream://127.0.0.1:5000/master;user=sqream;password=sqream";
    private static final String USER = "sqream";
    private static final String PASSWORD = "sqream";

    @Before
    public void setup() throws SQLException {
        Connection conn = createConenction();
        Statement statement = conn.createStatement();
        statement.execute(SQL_CREATE_TABLE);
        statement.close();
        conn.close();
    }

    @Test
    public void insertFromCSV() throws IOException, SQLException {
        File file = new File("/home/alexk/linesaa");
        CsvReader csvReader = new CsvReader();
        csvReader.setFieldSeparator('|');

        long t0 = System.currentTimeMillis();
        try(Connection conn = createConenction();
            PreparedStatement ps = conn.prepareStatement(SQL_INSERT);
            CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {

            CsvRow row;
            while ((row = csvParser.nextRow()) != null) {
                ps.setLong(1, Integer.parseInt(row.getField(0)));
                ps.setLong(2, Integer.parseInt(row.getField(1)));
                ps.setInt(3, Integer.parseInt(row.getField(2)));
                ps.setInt(4, Integer.parseInt(row.getField(3)));
                ps.setInt(5, Integer.parseInt(row.getField(4)));
                ps.setInt(6, Integer.parseInt(row.getField(5)));
                ps.setInt(7, Integer.parseInt(row.getField(6)));
                ps.setInt(8, Integer.parseInt(row.getField(7)));
                ps.setString(9, row.getField(8));
                ps.setString(10, row.getField(9));
                ps.setDate(11, Date.valueOf(row.getField(10)));
                ps.setDate(12, Date.valueOf(row.getField(11)));
                ps.setDate(13, Date.valueOf(row.getField(12)));
                ps.setString(14, row.getField(13));
                ps.setString(15, row.getField(14));
                ps.setString(16, row.getField(15));
                ps.addBatch();
            }
        }
        long t1 = System.currentTimeMillis();
        System.out.println(String.format("Result: %s sec", (t1 - t0) / 1_000));
    }

    private Connection createConenction() throws SQLException {
        return DriverManager.getConnection(URI, USER, PASSWORD);
    }
}
