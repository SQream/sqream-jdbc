import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.sql.Types;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;


public class NetworkInsertTool {

    private static final String NULL_ENTRY_STRING = "\\N";
    
    static void print(Object printable) {
        System.out.println(printable);
    }
    
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException{
        JDBCArgs arguments = new JDBCArgs(args);
        Class.forName("com.sqream.jdbc.SQDriver");
        insertCsvToTable(arguments);

    }

    private static void insertCsvToTable(JDBCArgs arguments) throws SQLException, IOException {

        try(Connection connection = DriverManager.getConnection(arguments.getConnectionURL(), arguments.user, arguments.password);
            Reader reader = Files.newBufferedReader(arguments.csvPath)){

            ArrayList<Integer> columnTypes = getColumnTypes(connection, arguments.schema, arguments.database, arguments.table);
            int numberOfColumns = columnTypes.size();
            
            try(PreparedStatement ps = connection.prepareStatement(buildInsertStatementBase(arguments, numberOfColumns));
                CSVReader csvReader = new CSVReaderBuilder(reader).withCSVParser(new CSVParserBuilder().withSeparator(arguments.delimiter).build()).build()){

                String[] csvLine;

                while ((csvLine = csvReader.readNext()) != null) {
                    for (int i=0; i < numberOfColumns; i++) {
                        // columns indices start with 1, hence the entryIndex: i + 1
                        preparedStatementSetColumnEntry(ps, i + 1, csvLine[i], columnTypes.get(i));
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }

    }

    private static ArrayList<Integer> getColumnTypes(Connection connection, String schema, String database, String table) throws SQLException{
        ResultSet columns = connection.getMetaData().getColumns(database, schema, table, "%");
        ArrayList<Integer> columnTypes = new ArrayList<>();
        while(columns.next()){
            columnTypes.add(columns.getInt("DATA_TYPE"));
        }
        return columnTypes;
    }

    private static String buildInsertStatementBase(JDBCArgs arguments, int numberOfColumns){
        String statement_base = "insert into " + arguments.table + " values (";
        for(int i = 0; i < numberOfColumns; i++){
            if(i < numberOfColumns - 1){
                statement_base += "?, ";
            } else{
                statement_base += "?";
            }
        }
        statement_base += ")";
        return statement_base;
    }

    private static void preparedStatementSetColumnEntry(PreparedStatement ps, int entryIndex, String entry, int columnType) throws SQLException{
        if(entry.equalsIgnoreCase(NULL_ENTRY_STRING)){
            ps.setNull(entryIndex, columnType);
            return;
        }

        switch(columnType){
            //https://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf?AuthParam=1558015624_39ea3110b30a6a0f04af8450c4971182 Page 191
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: ps.setString(entryIndex, entry); break;
            case Types.NUMERIC: ps.setBigDecimal(entryIndex, new BigDecimal(entry)); break;
            case Types.BIT:
            case Types.BOOLEAN: ps.setBoolean(entryIndex, Boolean.parseBoolean(entry)); break;
            case Types.TINYINT: ps.setByte(entryIndex, Byte.parseByte(entry)); break;
            case Types.SMALLINT: ps.setShort(entryIndex, Short.parseShort(entry)); break;
            case Types.INTEGER: ps.setInt(entryIndex, Integer.parseInt(entry)); break;
            case Types.BIGINT: ps.setLong(entryIndex, Long.parseLong(entry)); break;
            case Types.REAL: ps.setFloat(entryIndex, Float.parseFloat(entry)); break;
            case Types.DOUBLE: ps.setDouble(entryIndex, Double.parseDouble(entry)); break;
            case Types.DATE: ps.setDate(entryIndex, Date.valueOf(entry)); break;
            case Types.TIME: ps.setTime(entryIndex, Time.valueOf(entry)); break;
            case Types.TIMESTAMP: ps.setTimestamp(entryIndex, Timestamp.valueOf(entry)); break;

            default:
                throw new IllegalArgumentException("Invalid entry " + entry + " to column number " + entryIndex
                        + " of type " + Integer.toString(columnType) + " (refer to com.java.sql Types class for)");
        }
    }

    private static class JDBCArgs {

        Character delimiter;
        String ip;
        String port;
        String schema = "public";
        String database;
        String table;
        String user;
        String password;
        String service;
        Boolean cluster;
        Boolean ssl;
        Path csvPath;

        private static final String DEFAULT_IP = "127.0.0.1";

        private static final String DELIMITER_OPT = "del";
        private static final String DELIMITER_OPT_LONG = "delimiter";

        private static final String IP_OPT = "i";
        private static final String IP_OPT_LONG = "ip";

        private static final String PORT_OPT = "port";
        private static final String PORT_OPT_LONG = "port";

        private static final String DBNAME_OPT = "d";
        private static final String DBNAME_OPT_LONG = "database";
        
        private static final String SCHEMA_OPT = "s";
        private static final String SCHEMA_OPT_LONG = "schema";
        
        private static final String TABLENAME_OPT = "t";
        private static final String TABLENAME_OPT_LONG = "table";

        private static final String USER_OPT = "u";
        private static final String USER_OPT_LONG = "user";

        private static final String PASSWORD_OPT = "pw";
        private static final String PASSWORD_OPT_LONG = "pass";

        private static final String SERVICE_OPT = "se";
        private static final String SERVICE_OPT_LONG = "service";

        private static final String CLUSTER_OPT = "c";
        private static final String CLUSTER_OPT_LONG = "cluster";

        private static final String SSL_OPT = "ssl";
        private static final String SSL_OPT_LONG = "ssl";

        private static final String CSVPATH_OPT = "csv";
        private static final String CSVPATH_OPT_LONG = "csvpath";

        JDBCArgs(String[] args) {
            parse(args);
        }

        void parse(String[] args){
            Options options = getJDBCOptions();
            CommandLine cmd;

            try{
                CommandLineParser parser = new DefaultParser();
                cmd = parser.parse(options, args);

                String delimiter_str = cmd.getOptionValue(DELIMITER_OPT);

                if(delimiter_str.length() != 1){
                    throw new IllegalArgumentException("invalid delimiter char " + delimiter_str);
                } else{
                    delimiter = delimiter_str.charAt(0);
                }


                if(cmd.hasOption(IP_OPT)){
                    ip = cmd.getOptionValue(IP_OPT);
                } else{
                    ip = DEFAULT_IP;
                }

                port = cmd.getOptionValue(PORT_OPT);
                database = cmd.getOptionValue(DBNAME_OPT);
                schema = cmd.getOptionValue(SCHEMA_OPT);
                table = cmd.getOptionValue(TABLENAME_OPT);
                user = cmd.getOptionValue(USER_OPT);
                password = cmd.getOptionValue(PASSWORD_OPT);
                service = cmd.getOptionValue(SERVICE_OPT);
                cluster = cmd.hasOption(CLUSTER_OPT);
                ssl = cmd.hasOption(SSL_OPT);
                csvPath = Paths.get(cmd.getOptionValue(CSVPATH_OPT));

            } catch(Exception e){
                System.out.println(e.getMessage());
                e.printStackTrace();
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Network Insert Tool", options);

                System.exit(1);
            }
        }

        String getConnectionURL(){
            StringBuilder stringBuilder = new StringBuilder("jdbc:Sqream://");
            stringBuilder.append(ip);
            stringBuilder.append(":").append(port).append("/");
            stringBuilder.append(database);
            stringBuilder.append(";user=").append(user);
            stringBuilder.append(";password=").append(password);
            if(service != null){
                stringBuilder.append(";service").append(service);
            }
            if(cluster){
                stringBuilder.append(";cluster=true");
            } else{
                stringBuilder.append(";cluster=false");
            }
            if(ssl){
                stringBuilder.append(";ssl=true");
            }else{
                stringBuilder.append(";ssl=false");
            }

            return stringBuilder.toString();
        }

        private void addOption(String shortName, String longName, boolean hasArg, String description, boolean isRequired, Options options){
            Option option = new Option(shortName, longName, hasArg, description);
            option.setRequired(isRequired);
            options.addOption(option);
        }

        private Options getJDBCOptions(){
            Options options = new Options();

            addOption(DELIMITER_OPT, DELIMITER_OPT_LONG, true, "csv delimiter character", true, options);

            addOption(IP_OPT, IP_OPT_LONG, true, "sqreamd ip address", false, options);

            addOption(PORT_OPT, PORT_OPT_LONG, true, "sqreamd port", true, options);

            addOption(DBNAME_OPT, DBNAME_OPT_LONG, true, "sqream database name", true, options);

            addOption(SCHEMA_OPT, SCHEMA_OPT_LONG, true, "sqream schema name", false, options);

            addOption(TABLENAME_OPT, TABLENAME_OPT_LONG, true, "sqream table name", true, options);

            addOption(USER_OPT, USER_OPT_LONG, true, "sqream user", true, options);

            addOption(PASSWORD_OPT, PASSWORD_OPT_LONG, true, "sqream password", true, options);

            addOption(SERVICE_OPT, SERVICE_OPT_LONG, true, "sqream service", false, options);

            addOption(CLUSTER_OPT, CLUSTER_OPT_LONG, false, "is running on a cluster?", false, options);

            addOption(SSL_OPT, SSL_OPT_LONG, false, "enable ssl connection?", false, options);

            addOption(CSVPATH_OPT, CSVPATH_OPT_LONG, true, "table data csv path", true, options);

            return options;
        }

    }

}
