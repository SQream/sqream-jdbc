package com.sqream.jdbc.connector;

//Packing and unpacking columnar data
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// Socket communication

// More SSL shite

// JSON parsing library
import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import com.sqream.jdbc.connector.storage.ColumnStorage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.text.MessageFormat;

// Datatypes for building columns and other
import java.util.BitSet;

// Unicode related
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

// Date / Time related
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
// Aux
import java.util.Arrays;   //  To allow debug prints via Arrays.toString

//Exceptions
import javax.script.ScriptException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

// SSL over SocketChannel abstraction

import static com.sqream.jdbc.connector.enums.StatementType.*;

public class ConnectorImpl implements Connector {
    private static final Logger LOGGER = Logger.getLogger(ConnectorImpl.class.getName());

    private static final String DEFAULT_CHARACTER_CODES = "ascii";
    private static final String DEFAULT_SERVICE = "sqream";
    private static final String DEFAULT_USER = "sqream";
    private static final String DEFAULT_PASSWORD = "sqream";
    // Date/Time conversion related
    private static final ZoneId SYSTEM_TZ = ZoneId.systemDefault();
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private static final int BYTE_BUFFER_POOL = 3;

    private SQSocketConnector socket;
    private Messenger messenger;

    private int connection_id = -1;
    private int statementId = -1;
    private String varchar_encoding = DEFAULT_CHARACTER_CODES;  // default encoding/decoding for varchar columns

    private String database;
    private String user = DEFAULT_USER;
    private String password = DEFAULT_PASSWORD;
    private String service = DEFAULT_SERVICE;
    private boolean useSsl;

    // Binary data related
    int ROWS_PER_FLUSH = 100000;
    private int rows_per_flush;

    // Column metadata
    private StatementType statement_type;
    private int row_length;

    private TableMetadata tableMetadata;
    private ColumnStorage colStorage;

    private boolean openStatement = false;

    // Column Storage
    private List<BlockDto> queue = new ArrayList<>();
    private List<Integer> rows_per_batch = new ArrayList<>();
    private int rows_in_current_batch;
    private int fetch_limit = 0;

    // Get / Set related
    private int rowCounter, totalRowCounter;
    private BitSet columns_set;

    private byte[] string_bytes; // Storing converted string to be set

    private int[] col_calls;

    // Managing stop_statement
    private AtomicBoolean IsCancelStatement = new AtomicBoolean(false);

    private InsertValidator validator;
    private FlushService flushService;
    private ByteBufferPool byteBufferPool;

    public ConnectorImpl(String ip, int port, boolean cluster, boolean ssl) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        /* JSON parsing engine setup, initial socket connection */
        useSsl = ssl;
        socket = SQSocketConnector.connect(ip, port, useSsl);
        // Clustered connection - reconnect to actual ip and port
        if (cluster) {
            reconnectToNode();
        }
        this.messenger = MessengerImpl.getInstance(socket);
        this.flushService = FlushService.getInstance(socket, messenger);
    }

    private void reconnectToNode() throws NoSuchAlgorithmException, IOException, KeyManagementException {
        ByteBuffer response_buffer = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
        // Get data from server picker
        response_buffer.clear();
        //_read_data(response_buffer, 0); // IP address size may vary
        int bytes_read = socket.read(response_buffer);
        response_buffer.flip();
        if (bytes_read == -1) {
            throw new IOException("Socket closed When trying to connect to server picker");
        }

        // Read size of IP address (7-15 bytes) and get the IP
        byte [] ip_bytes = new byte[response_buffer.getInt()]; // Retreiving ip from clustered connection
        response_buffer.get(ip_bytes);
        String ip = new String(ip_bytes, UTF8);

        // Last is the port
        int port = response_buffer.getInt();

        socket.reconnect(ip, port, useSsl);
    }

    // Internal Mechanism Functions
    // ----------------------------
    /*
     * (1) _parse_sqream_json -        Return Map from a JSON string
     * (2) _generate_headered_buffer - Create ByteBuffer for message and fill the header
     * (3)  _parse_response_header -   Extract header info from received ByteBuffer
     * (4)  _send_data -
     * (5) _send_message -
     */


    // Internal API Functions
    // ----------------------------
    /*
     * (1) _parse_query_type() -
     * (2) _flush()
     */

    // ()  /* Unpack the json of column data arriving via queryType/(named). Called by prepare()  */
    //@SuppressWarnings("rawtypes") // "Map is a raw type" @ col_data = (Map)query_type.get(idx);
    private void parseQueryType(List<ColumnMetadataDto> columnsMetadata) throws IOException, ScriptException{

        row_length = columnsMetadata.size();
        if(row_length ==0) {
            return;
        }

        tableMetadata = TableMetadata.builder()
                .rowLength(row_length)
                .fromColumnsMetadata(columnsMetadata)
                .statementType(statement_type)
                .build();

        validator = new InsertValidator(tableMetadata);

        col_calls = new int[row_length];

        // Create Storage for insert / select operations
        if (statement_type.equals(INSERT)) {
            // Calculate number of rows to flush at
            int row_size = tableMetadata.getSizesSum() + tableMetadata.getAmountNullablleColumns();    // not calculating nvarc lengths for now
            rows_per_flush = ROWS_PER_FLUSH;

            // Instantiate flags for managing network insert operations
            rowCounter = 0;
            columns_set = new BitSet(row_length); // defaults to false

            // Initiate buffers for each column using the metadata
            colStorage = ColumnStorage.builder()
                    .metadata(tableMetadata)
                    .blockSize(ROWS_PER_FLUSH)
                    .build();

            byteBufferPool = new ByteBufferPool(BYTE_BUFFER_POOL, ROWS_PER_FLUSH, tableMetadata);
        }
        if (statement_type.equals(SELECT)) {

            // Instantiate select counters, Initial storage same as insert
            rowCounter = -1;
            totalRowCounter = 0;
            int total_rows_fetched = -1;

            // Get the maximal string size (or size fo another type if strings are very small)
            string_bytes = new byte[tableMetadata.getMaxSize()];
        }
    }

    private int _fetch() throws IOException, ScriptException, ConnException {
        /* Request and get data from SQream following a SELECT query */

        // Send fetch request and get metadata on data to be received

        FetchMetadataDto fetchMeta = messenger.fetch();

        if (fetchMeta.getNewRowsFetched() == 0) {
            close();  // Auto closing statement if done fetching
            return fetchMeta.getNewRowsFetched();
        }
        // Initiate storage columns using the "colSzs" returned by SQream
        // All buffers in a single array to use SocketChannel's read(ByteBuffer[] dsts)
        ByteBuffer[] fetch_buffers = new ByteBuffer[fetchMeta.colAmount()];

        for (int i=0; i < fetchMeta.colAmount(); i++) {
            fetch_buffers[i] = ByteBuffer.allocateDirect(fetchMeta.getSizeByIndex(i)).order(ByteOrder.LITTLE_ENDIAN);
        }

        rows_per_batch.add(fetchMeta.getNewRowsFetched());

        // Initial naive implememntation - Get all socket data in advance
        int bytes_read = socket.getParseHeader();   // Get header out of the way
        for (ByteBuffer fetched : fetch_buffers) {
            socket.readData(fetched, fetched.capacity());
            //Arrays.stream(fetch_buffers).forEach(fetched -> fetched.flip());
        }

        // Sort buffers to appropriate arrays (row_length determied during _query_type())
        colStorage = ColumnStorage.builder()
                .metadata(tableMetadata)
                .blockSize(ROWS_PER_FLUSH)
                .fromFetchBuffers(fetch_buffers)
                .build();

        // Add buffers to buffer list
        queue.add(colStorage.getBlock());

        return fetchMeta.getNewRowsFetched();  // counter nullified by next()
    }


    private int _fetch(int row_amount) throws IOException, ScriptException, ConnException {
    	int total_fetched = 0;
    	int new_rows_fetched;

    	if (row_amount < -1) {
    		throw new ConnException("row_amount should be positive, got " + row_amount);
    	}
    	if (row_amount == -1) {
    		// Place for adding logic for previos fetching behavior - per
    		// requirement fetch
    	}
    	else {  // positive row amount
    		while (row_amount == 0 || total_fetched < row_amount) {
    			new_rows_fetched = _fetch();
    			if (new_rows_fetched ==0)
    				break;
    			total_fetched += new_rows_fetched;
    		}
    	}
    	close();

    	rows_in_current_batch = 0;

    	return total_fetched;
    }



    private int _flush(int row_counter, boolean isAsyncFlush) throws IOException, ConnException {
        /* Send columnar data buffers to SQream. Called by next() and close() */

        if (!statement_type.equals(INSERT) || row_counter == 0) {  // Not an insert statement
            return 0;
        }

        BlockDto blockAfterFlush = flushService.process(
                row_length,
                rowCounter,
                tableMetadata,
                colStorage.getBlock(),
                colStorage.getTotalLengthForHeader(row_length, row_counter),
                byteBufferPool,
                isAsyncFlush);

        colStorage.setBlock(blockAfterFlush);

        return row_counter;  // counter nullified by next()
    }

    // User API Functions
    /* ------------------
     * connect(), execute(), next(), close(), close_conenction()
     *
     */
    @Override
    public int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnException {
        //"'{'\"username\":\"{0}\", \"password\":\"{1}\", \"connectDatabase\":\"{2}\", \"service\":\"{3}\"'}'";

        database = _database;
        user = _user;
        password = _password;
        service = _service;

        ConnectionStateDto connState = messenger.connect(database, user, password, service);
        connection_id = connState.getConnectionId();
        varchar_encoding = connState.getVarcharEncoding();

        return connection_id;
    }

    @Override
    public int execute(String statement) throws IOException, ScriptException, ConnException, KeyManagementException, NoSuchAlgorithmException {
        /* Retains behavior of original execute()  */

        int default_chunksize = (int) Math.pow(10,6);
        return execute(statement, default_chunksize);
    }

    @Override
    public int execute(String statement, int chunkSize) throws IOException, ScriptException, ConnException, NoSuchAlgorithmException, KeyManagementException {
        LOGGER.log(Level.FINE, MessageFormat.format("Statement=[{0}], ChunkSize=[{1}]", statement, chunkSize));

        if (chunkSize < 0) {
            throw new ConnException("chunk_size should be positive, got " + chunkSize);
        }
        /* getStatementId, prepareStatement, reconnect, execute, queryType  */
        boolean charitable = true;
        if (openStatement) {
            if (charitable) {  // Automatically close previous unclosed statement
                close();
            } else {
                throw new ConnException("Trying to run a statement when another was not closed. Open statement id: " + statementId + " on connection: " + connection_id);
            }
        }
        openStatement = true;
        // Get statement ID, send prepareStatement and get response parameters
        statementId = messenger.openStatement();

        StatementStateDto statementState = messenger.prepareStatement(statement, chunkSize);


        int port = useSsl ? statementState.getPortSsl() : statementState.getPort();
        // Reconnect and reestablish statement if redirected by load balancer
        if (statementState.isReconnect()) {
            socket.reconnect(statementState.getIp(), port, useSsl);

            // Sending reconnect, reconstruct commands
            messenger.reconnect(database, user, password, service, connection_id, statementState.getListenerId());
            messenger.isStatementReconstructed(statementId);
        }

        // Getting query type manouver and setting the type of query
        messenger.execute();
        List<ColumnMetadataDto> queryType = messenger.queryTypeInput();

        if (queryType.isEmpty()) {
            queryType = messenger.queryTypeOut();
            statement_type = queryType.isEmpty() ? DML : SELECT;
        }
        else {
            statement_type = INSERT;
        }

        // Select or Insert statement - parse queryType response for metadata
        if (!statement_type.equals(DML)) {
            parseQueryType(queryType);
        }
        // First fetch on the house, auto close statement if no data returned
        if (statement_type.equals(SELECT)) {
            int total_rows_fetched = _fetch(fetch_limit); // 0 - prefetch all data
            //if (total_rows_fetched < (chunk_size == 0 ? 1 : chunk_size)) {
        }

        return statementId;
    }

    @Override
    public boolean next() throws ConnException, IOException, ScriptException {
        /* See that all needed buffers were set, flush if needed, nullify relevant
           counters */

        if (statement_type.equals(INSERT)) {

            // Were all columns set
            //if (!IntStream.range(0, columns_set.length).allMatch(i -> columns_set[i]))
            if (columns_set.cardinality() < row_length)
                throw new ConnException ("All columns must be set before calling next(). Set " + columns_set.cardinality() +  " columns out of "  + row_length);

            // Nullify column flags and update counter
            columns_set.clear();
            rowCounter++;

            // Flush and clean if needed
            if (rowCounter == rows_per_flush) {
                _flush(rowCounter, true);

                // After flush, clear row counter and all buffers
                rowCounter = 0;
                colStorage.clearBuffers(row_length);
            }
        }
        else if (statement_type.equals(SELECT)) {
            //print ("select row counter: " + row_counter + " total: " + total_rows_fetched);
        	Arrays.fill(col_calls, 0); // calls in the same fetch - for varchar / nvarchar
        	if (fetch_limit !=0 && totalRowCounter == fetch_limit)
        		return false;  // MaxRow limit reached, stop even if more data was fetched
        	// If all data has been read, try to fetch more
        	if (rowCounter == (rows_in_current_batch -1)) {
        		rowCounter = -1;
        		if (rows_per_batch.size() == 0)
                    return false; // No more data and we've read all we have

                // Set new active buffer to be reading data from
                rows_in_current_batch = rows_per_batch.get(0);
                colStorage.loadBlock(queue.get(0));

                // Remove active buffer from list
                queue.remove(0);
                rows_per_batch.remove(0);
            }
            rowCounter++;
            totalRowCounter++;
        }
        else if (statement_type.equals(DML))
            throw new ConnException ("Calling next() on a non insert / select query");

        else
            throw new ConnException ("Calling next() on a statement type different than INSERT / SELECT / DML: " + statement_type.getValue());

        return true;
    }

    @Override
    public void close() throws IOException, ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Close statement: openStatement=[{0}], statementType=[{1}]]", openStatement, statement_type));

    	if (isOpen()) {
    		if (openStatement) {
                flushService.awaitTermination();
    			if (statement_type!= null && statement_type.equals(INSERT)) {
    	            _flush(rowCounter, false);
    	        }
    	            // Statement is finished so no need to reset row_counter etc
                messenger.closeStatement();
                openStatement = false;  // set to true in execute()
            }
        }
    }

    @Override
    public boolean closeConnection() throws IOException, ScriptException, ConnException {
        if (isOpen()) {
            if (openStatement) { // Close open statement if exists
                close();
            }
            messenger.closeConnection();
            socket.close();
        }
        return true;
    }

    // Gets
    // ----

    // -o-o-o-o-o    By index -o-o-o-o-o
    @Override
    public Boolean getBoolean(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getBoolean(colNum - 1, rowCounter);
    }

    @Override
    public Byte get_ubyte(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getUbyte(colNum - 1, rowCounter);
    }

    @Override
    public Short get_short(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getShort(colNum - 1, rowCounter);
    }

    @Override
    public Integer get_int(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getInt(colNum - 1, rowCounter);
    }

    @Override
    public Long get_long(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getLong(colNum - 1, rowCounter);
    }

    @Override
    public Float get_float(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getFloat(colNum - 1, rowCounter);
    }

    @Override
    public Double get_double(int colNum) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getDouble(colNum - 1, rowCounter);
    }

    @Override
    public String get_varchar(int colNum) {
        int colIndex = colNum - 1;
        validator.validateColumnIndex(colIndex);
        boolean repeatedly = col_calls[colIndex]++ > 0;
        return colStorage.getVarchar(colIndex, rowCounter, varchar_encoding, repeatedly);
    }

    @Override
    public String get_nvarchar(int colNum) throws ConnException {
        int colIndex = colNum - 1;
        validator.validateColumnIndex(colIndex);
        boolean repeatedly = col_calls[colIndex]++ > 0;
        return colStorage.getNvarchar(colIndex, rowCounter, UTF8, repeatedly);
    }

    @Override
    public Date get_date(int colNum, ZoneId zone) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getDate(colNum - 1, rowCounter, zone);
    }

    @Override
    public Timestamp get_datetime(int colNum, ZoneId zone) throws ConnException {
        validator.validateColumnIndex(colNum - 1);
        return colStorage.getTimestamp(colNum - 1, rowCounter, zone);
    }

    @Override
    public Date get_date(int colNum) throws ConnException {
        return get_date(colNum, SYSTEM_TZ); // system_tz, UTC
    }

    @Override
    public Timestamp get_datetime(int colNum) throws ConnException {
        return get_datetime(colNum, SYSTEM_TZ); // system_tz, UTC
    }

    // -o-o-o-o-o  By column name -o-o-o-o-o
    @Override
    public Boolean getBoolean(String col_name) throws ConnException {

        return getBoolean(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Byte get_ubyte(String col_name) throws ConnException {

        return get_ubyte(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Short get_short(String col_name) throws ConnException {

        return get_short(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Integer get_int(String col_name) throws ConnException {

        return get_int(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Long get_long(String col_name) throws ConnException {

        return get_long(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Float get_float(String col_name) throws ConnException {

        return get_float(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Double get_double(String col_name) throws ConnException {

        return get_double(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public String get_varchar(String col_name) throws ConnException, UnsupportedEncodingException {

        return get_varchar(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public String get_nvarchar(String col_name) throws ConnException {

        return get_nvarchar(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Date get_date(String col_name) throws ConnException {

        return get_date(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Date get_date(String col_name, ZoneId zone) throws ConnException {

        return get_date(tableMetadata.getColNumByName(col_name), zone);
    }

    @Override
    public Timestamp get_datetime(String col_name) throws ConnException {

        return get_datetime(tableMetadata.getColNumByName(col_name));
    }

    @Override
    public Timestamp get_datetime(String col_name, ZoneId zone) throws ConnException {

        return get_datetime(tableMetadata.getColNumByName(col_name), zone);
    }

    // Sets
    // ----

    @Override
    public boolean set_boolean(int colNum, Boolean value) throws ConnException {
        validator.validateSet(colNum - 1, value, "ftBool");
        colStorage.setBoolean(colNum - 1, value);
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_ubyte(int colNum, Byte value) throws ConnException {
        return setUbyte(colNum, value, true);
    }

    private boolean setUbyte(int colNum, Byte value, boolean validateValue) {
        validator.validateSet(colNum - 1, value, "ftUByte");
        if (validateValue) {
            validator.validateUbyte(value);
        }
        colStorage.setUbyte(colNum - 1, value);
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_short(int colNum, Short value) throws ConnException {
        if ("ftUByte".equals(tableMetadata.getType(colNum - 1))) {
            if (value < 0 || value > 255) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "Trying to set wrong value [{0}] on an unsigned byte column", value));
            }
            setUbyte(colNum, value.byteValue(), false);
        } else {
            validator.validateSet(colNum - 1, value, "ftShort");
            colStorage.setShort(colNum - 1, value);
            // Mark column as set (BitSet at location col_num set to true
            columns_set.set(colNum - 1);
        }

        return true;
    }

    @Override
    public boolean set_int(int colNum, Integer value) throws ConnException {
        validator.validateSet(colNum - 1, value, "ftInt");
        colStorage.setInt(colNum - 1, value);
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_long(int colNum, Long value) throws ConnException {
        validator.validateSet(colNum - 1, value, "ftLong");
        colStorage.setLong(colNum - 1, value);
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_float(int colNum, Float value) throws ConnException {
        validator.validateSet(colNum - 1, value, "ftFloat");
        colStorage.setFloat(colNum - 1, value);
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_double(int colNum, Double value) throws ConnException {
        validator.validateSet(colNum - 1, value, "ftDouble");
        colStorage.setDouble(colNum - 1, value);
        // Mark column as set
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_varchar(int colNum, String value) throws ConnException, UnsupportedEncodingException {
        validator.validateSet(colNum - 1, value, "ftVarchar");
        // converting to byte array before validation
        byte[] stringBytes = value == null ? "".getBytes(varchar_encoding) : value.getBytes(varchar_encoding);
        validator.validateVarchar(colNum - 1, stringBytes.length);
        colStorage.setVarchar(colNum - 1, stringBytes, value);
        // Mark column as set
        columns_set.set(colNum -1);
        return true;
    }

    @Override
    public boolean set_nvarchar(int colNum, String value) throws ConnException, UnsupportedEncodingException {
        validator.validateSet(colNum - 1, value, "ftBlob");
        // Convert string to bytes
        byte[] stringBytes = value == null ? "".getBytes(UTF8) : value.getBytes(UTF8);
        colStorage.setNvarchar(colNum - 1, stringBytes, value);
        // Mark column as set
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_date(int colNum, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException {
        validator.validateSet(colNum - 1, date, "ftDate");
        colStorage.setDate(colNum - 1, date, zone);
        // Mark column as set
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_datetime(int colNum, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException {
        validator.validateSet(colNum - 1, ts, "ftDateTime");
        colStorage.setDatetime(colNum - 1, ts, zone);
        // Mark column as set
        columns_set.set(colNum - 1);
        return true;
    }

    @Override
    public boolean set_date(int col_num, Date value) throws ConnException, UnsupportedEncodingException {
        return set_date(col_num, value, SYSTEM_TZ); // system_tz, UTC
    }

    @Override
    public boolean set_datetime(int col_num, Timestamp value) throws ConnException, UnsupportedEncodingException {
        return set_datetime(col_num, value, SYSTEM_TZ); // system_tz, UTC
    }

    // Metadata
    // --------

    int _validate_col_num(int col_num) throws ConnException {

        if (col_num <1)
            throw new ConnException ("Using a metadata function with a non positive column value");

        return --col_num;
    }

    @Override
    public int getStatementId() {
        return statementId;
    }

    @Override
    public String getQueryType() {
        return statement_type.getValue();
    }

    @Override
    public int getRowLength() {  // number of columns for this query

        return row_length;
    }

    @Override
    public String getColName(int col_num) throws ConnException {

        return tableMetadata.getName(_validate_col_num(col_num));
    }

    @Override
    public String get_col_type(int col_num) throws ConnException {
        return tableMetadata.getType(_validate_col_num(col_num));
    }

    @Override
    public String get_col_type(String col_name) throws ConnException {
        Integer colNum = tableMetadata.getColNumByName(col_name);
        if (colNum == null)
            throw new ConnException("\nno column found for name: " + col_name + "\nExisting columns: \n" + tableMetadata.getAllNames());
        return get_col_type(colNum);
    }

    @Override
    public int get_col_size(int col_num) throws ConnException {

        return tableMetadata.getSize(_validate_col_num(col_num));
    }

    @Override
    public boolean is_col_nullable(int col_num) throws ConnException {

        return tableMetadata.isNullable(_validate_col_num(col_num));
    }

    @Override
    public boolean isOpenStatement() {
        return openStatement;
    }

    @Override
    public boolean isOpen() {
        return socket.isOpen();
    }

    @Override
    public AtomicBoolean checkCancelStatement() {
        return this.IsCancelStatement;
    }

    @Override
    public void setOpenStatement(boolean openStatement) {
        this.openStatement = openStatement;
    }

    @Override
    public boolean setFetchLimit(int _fetch_limit) throws ConnException{

        if (_fetch_limit < 0)
            throw new ConnException("Max rows to fetch should be nonnegative, got" + _fetch_limit);

        fetch_limit = _fetch_limit;

        return true;
    }

    @Override
    public int getFetchLimit() {
        return fetch_limit;
    }

}
