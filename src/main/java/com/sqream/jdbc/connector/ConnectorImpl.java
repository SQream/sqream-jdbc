package com.sqream.jdbc.connector;

//Packing and unpacking columnar data
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// Socket communication

// More SSL shite

// JSON parsing library
import com.eclipsesource.json.ParseException;
import com.eclipsesource.json.WriterConfig;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.sqream.jdbc.connector.enums.StatementType;

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

// SSL over SocketChannel abstraction

import static com.sqream.jdbc.connector.enums.StatementType.*;
import static com.sqream.jdbc.utils.Utils.*;

public class ConnectorImpl implements Connector {

    private static final String DEFAULT_CHARACTER_CODES = "ascii";
    private static final String DEFAULT_SERVICE = "sqream";
    private static final String DEFAULT_USER = "sqream";
    private static final String DEFAULT_PASSWORD = "sqream";
    // Date/Time conversion related
    private static final ZoneId SYSTEM_TZ = ZoneId.systemDefault();
    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final String DENIED = "denied";

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
    private static final int FLUSH_SIZE = 10_000_000;
    int ROWS_PER_FLUSH = 100000;
    int TEXT_ITEM_SIZE = (int) Math.pow(10, 5);
    private int rows_per_flush;

    // Column metadata
    private StatementType statement_type;
    private int row_length;

    private ColumnsMetadata colMetadata;
    private ColumnStorage colStorage;
    private JsonParser jsonParser;

    private boolean openStatement = false;

    // Column Storage
    private List<ByteBuffer[]> data_buffers = new ArrayList<>();
    private List<ByteBuffer[]> null_buffers = new ArrayList<>();
    private List<ByteBuffer []> nvarc_len_buffers = new ArrayList<>();
    private List<Integer> rows_per_batch = new ArrayList<>();
    private int rows_in_current_batch;
    private int fetch_limit = 0;

    // Get / Set related
    private int row_counter, total_row_counter;
    private BitSet columns_set;

    private byte[] string_bytes; // Storing converted string to be set

    private int[] col_calls;

    // Managing stop_statement
    private AtomicBoolean IsCancelStatement = new AtomicBoolean(false);

    public ConnectorImpl(String ip, int port, boolean cluster, boolean ssl) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        /* JSON parsing engine setup, initial socket connection */
        useSsl = ssl;
        socket = new SQSocketConnector(ip, port);
        socket.connect(useSsl);
        // Clustered connection - reconnect to actual ip and port
        if (cluster) {
            reconnectToNode();
        }
        this.messenger = new Messenger(socket);
        this.colMetadata = new ColumnsMetadata();
        this.colStorage = new ColumnStorage();
        this.jsonParser = new JsonParser();
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

    private JsonObject parseJson(String jsonStr) {
        return Json.parse(jsonStr).asObject();
    }

    // Internal API Functions
    // ----------------------------
    /*
     * (1) _parse_query_type() -
     * (2) _flush()
     */

    // ()  /* Unpack the json of column data arriving via queryType/(named). Called by prepare()  */
    //@SuppressWarnings("rawtypes") // "Map is a raw type" @ col_data = (Map)query_type.get(idx);
    private void parseQueryType(List<ColumnMetadataDto> queryType) throws IOException, ScriptException{

        row_length = queryType.size();
        if(row_length ==0) {
            return;
        }
        // Set metadata arrays given the amount of columns
        colMetadata.init(row_length);

        col_calls = new int[row_length];
        // Parse the queryType json to get metadata for every column
        // An internal item looks like: {"isTrueVarChar":false,"nullable":true,"type":["ftInt",4,0]}
        for(int i=0; i < row_length; i++) {
            // Parse JSON to correct objects
            ColumnMetadataDto colMetaDataDto = queryType.get(i);

            if (!statement_type.equals(SELECT)) {
                colMetaDataDto.setName(DENIED);
            }

            // Assign data from parsed JSON objects to metadata arrays
            colMetadata.setByIndex(i, colMetaDataDto);
        }

        // Create Storage for insert / select operations
        if (statement_type.equals(INSERT)) {
            // Calculate number of rows to flush at
            int row_size = colMetadata.getSizesSum() + colMetadata.getAmountNullablleColumns();    // not calculating nvarc lengths for now
            rows_per_flush = ROWS_PER_FLUSH;
            colStorage.init(row_length);
            colStorage.setNullReseter(rows_per_flush);

            // Instantiate flags for managing network insert operations
            row_counter = 0;
            columns_set = new BitSet(row_length); // defaults to false

            // Initiate buffers for each column using the metadata
            for (int idx=0; idx < row_length; idx++) {
                colStorage.initDataColumns(idx, colMetadata.getSize(idx)*rows_per_flush);
                if (colMetadata.isNullable(idx)) {
                    colStorage.initNullColumns(idx, rows_per_flush);
                } else {
                    colStorage.resetNullColumns(idx);
                }
                if (colMetadata.isTruVarchar(idx)) {
                    colStorage.initNvarcLenColumns(idx, rows_per_flush);
                } else {
                    colStorage.resetNvarcLenColumns(idx);
                }
            }
        }
        if (statement_type.equals(SELECT)) {

            // Instantiate select counters, Initial storage same as insert
            row_counter = -1;
            total_row_counter = 0;
            int total_rows_fetched = -1;

            // Get the maximal string size (or size fo another type if strings are very small)
            string_bytes = new byte[colMetadata.getMaxSize()];
        }
    }

    private int _fetch() throws IOException, ScriptException, ConnException {
        /* Request and get data from SQream following a SELECT query */

        // Send fetch request and get metadata on data to be received

        FetchMetadataDto fetchMeta = jsonParser.toFetchMetadata(messenger.fetch());

        if (fetchMeta.getNewRowsFetched() == 0) {
            close();  // Auto closing statement if done fetching
            return fetchMeta.getNewRowsFetched();
        }
        // Initiate storage columns using the "colSzs" returned by SQream
        // All buffers in a single array to use SocketChannel's read(ByteBuffer[] dsts)
        int col_buf_size;
        ByteBuffer[] fetch_buffers = new ByteBuffer[fetchMeta.colAmount()];
        colStorage.init(row_length);

        for (int i=0; i < fetchMeta.colAmount(); i++) {
            fetch_buffers[i] = ByteBuffer.allocateDirect(fetchMeta.getSizeByIndex(i)).order(ByteOrder.LITTLE_ENDIAN);
        }
        // Sort buffers to appropriate arrays (row_length determied during _query_type())
        for (int idx=0, buf_idx = 0; idx < row_length; idx++, buf_idx++) {
            if(colMetadata.isNullable(idx)) {
                colStorage.setNullColumns(idx, fetch_buffers[buf_idx]);
                buf_idx++;
            } else {
                colStorage.resetNullColumns(idx);
            }
            if(colMetadata.isTruVarchar(idx)) {
                colStorage.setNvarcLenColumns(idx, fetch_buffers[buf_idx]);
                buf_idx++;
            } else {
                colStorage.resetNvarcLenColumns(idx);
            }
            colStorage.setDataColumns(idx, fetch_buffers[buf_idx]);
        }

        // Add buffers to buffer list
        data_buffers.add(colStorage.getDataColumns());
        null_buffers.add(colStorage.getNullColumns());
        nvarc_len_buffers.add(colStorage.getNvarcLenColumns());
        rows_per_batch.add(fetchMeta.getNewRowsFetched());

        // Initial naive implememntation - Get all socket data in advance
        int bytes_read = socket.getParseHeader();   // Get header out of the way
        for (ByteBuffer fetched : fetch_buffers) {
            socket.readData(fetched, fetched.capacity());
            //Arrays.stream(fetch_buffers).forEach(fetched -> fetched.flip());
        }

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



    private int _flush(int row_counter) throws IOException, ConnException {
        /* Send columnar data buffers to SQream. Called by next() and close() */

        if (!statement_type.equals(INSERT) || row_counter == 0) {  // Not an insert statement
            return 0;
        }

        // Send put message
        messenger.put(row_counter);

        // Get total column length for the header
        int total_bytes = colStorage.getTotalLengthForHeader(row_length, row_counter);

        // Send header with total binary insert
        ByteBuffer header_buffer = socket.generateHeaderedBuffer(total_bytes, false);
        socket.sendData(header_buffer, false);

        // Send available columns
        for(int idx=0; idx < row_length; idx++) {
            if(colStorage.getNullColumn(idx) != null) {
                socket.sendData((ByteBuffer) colStorage.getNullColumn(idx).position(row_counter), false);
            }
            if(colStorage.getNvarcLenColumn(idx) != null) {
                socket.sendData(colStorage.getNvarcLenColumn(idx), false);
            }
            socket.sendData(colStorage.getDataColumns(idx), false);
        }
        messenger.isPutted();
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

        ConnectionStateDto connState = jsonParser.toConnectionState(messenger.connect(database, user, password, service));
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
        statementId = jsonParser.toStatementId(messenger.getStatementId());

        // Generating a valid json string via external library
        JsonObject prepare_jsonify;
        try
        {
            prepare_jsonify = Json.object()
                    .add("prepareStatement", statement)
                    .add("chunkSize", chunkSize);
        }
        catch(ParseException e)
        {
            throw new ConnException ("Could not parse the statement for PrepareStatement");
        }
        // Jsonifying via standard library - verify with test
        //engine_bindings.put("statement", statement);
        //String prepareStr = (String) engine.eval("JSON.stringify({prepareStatement: statement, chunkSize: 0})");
        String prepareStr = prepare_jsonify.toString(WriterConfig.MINIMAL);

        StatementStateDto statementState = jsonParser.toStatementState(socket.sendMessage(prepareStr, true));

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
        List<ColumnMetadataDto> queryType = jsonParser.toQueryTypeInput(messenger.queryTypeInput());

        if (queryType.isEmpty()) {
            queryType = jsonParser.toQueryTypeOut(messenger.queryTypeOut());
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
            row_counter++;

            // Flush and clean if needed
            if (row_counter == rows_per_flush) {
                _flush(row_counter);

                // After flush, clear row counter and all buffers
                row_counter = 0;
                colStorage.clearBuffers(row_length);
            }
        }
        else if (statement_type.equals(SELECT)) {
            //print ("select row counter: " + row_counter + " total: " + total_rows_fetched);
        	Arrays.fill(col_calls, 0); // calls in the same fetch - for varchar / nvarchar
        	if (fetch_limit !=0 && total_row_counter == fetch_limit)
        		return false;  // MaxRow limit reached, stop even if more data was fetched
        	// If all data has been read, try to fetch more
        	if (row_counter == (rows_in_current_batch -1)) {
        		row_counter = -1;
        		if (rows_per_batch.size() == 0)
                    return false; // No more data and we've read all we have

                // Set new active buffer to be reading data from
                rows_in_current_batch = rows_per_batch.get(0);
                colStorage.reload(
                        data_buffers.get(0),
                        null_buffers.get(0),
                        nvarc_len_buffers.get(0)
                );

                // Remove active buffer from list
                data_buffers.remove(0);
                null_buffers.remove(0);
                nvarc_len_buffers.remove(0);
                rows_per_batch.remove(0);
            }
            row_counter++;
            total_row_counter++;
        }
        else if (statement_type.equals(DML))
            throw new ConnException ("Calling next() on a non insert / select query");

        else
            throw new ConnException ("Calling next() on a statement type different than INSERT / SELECT / DML: " + statement_type.getValue());

        return true;
    }

    @Override
    public Boolean close() throws IOException, ConnException {

    	String res = "";

    	if (isOpen()) {
    		if (openStatement) {

    			if (statement_type!= null && statement_type.equals(INSERT)) {
    	            _flush(row_counter);
    	        }
    	            // Statement is finished so no need to reset row_counter etc

                res = messenger.closeStatement();
                openStatement = false;  // set to true in execute()
            }
            else
                return false;  //res =  "statement " + statement_id + " already closed";
        }
        else
            return false;  //res =  "connection already closed";

        return true;
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

    private boolean _validate_index(int col_num) throws ConnException {
        if (col_num <0 || col_num >= row_length) {
            throw new ConnException(MessageFormat.format(
                    "Illegal index [{0}] on get/set\nAllowed indices are [0-{1}]", col_num, (row_length - 1)));
        }
    	return true;
    }

    // Gets
    // ----

    boolean _validate_get(int col_num, String value_type) throws ConnException {
        /* If get function is appropriate, return true for non null values, false for a null */
        // Validate type
        if (!colMetadata.getType(col_num).equals(value_type))
            throw new ConnException("Trying to get a value of type " + value_type + " from column number " + col_num + " of type " + colMetadata.getType(col_num));

        // print ("null column holder: " + Arrays.toString(null_columns));
        return colStorage.getNullColumn(col_num) == null || colStorage.getNullColumn(col_num).get(row_counter) == 0;
    }

    // -o-o-o-o-o    By index -o-o-o-o-o
    @Override
    public Boolean getBoolean(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        return (_validate_get(col_num, "ftBool")) ? colStorage.getDataColumns(col_num).get(row_counter) != 0 : null;
    }

    @Override
    public Byte get_ubyte(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        return (_validate_get(col_num, "ftUByte")) ? colStorage.getDataColumns(col_num).get(row_counter) : null;
    }  // .get().toUnsignedInt()  -->  to allow values between 127-255

    @Override
    public Short get_short(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        //*
        if (colMetadata.getType(col_num).equals("ftUByte"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (short)(colStorage.getDataColumns(col_num).get(row_counter) & 0xFF) : null;
        //*/

        return (_validate_get(col_num, "ftShort")) ? colStorage.getDataColumns(col_num).getShort(row_counter * 2) : null;
    }

    @Override
    public Integer get_int(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        //*
        if (colMetadata.getType(col_num).equals("ftShort"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (int) colStorage.getDataColumns(col_num).getShort(row_counter * 2) : null;
        else if (colMetadata.getType(col_num).equals("ftUByte"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (int)(colStorage.getDataColumns(col_num).get(row_counter) & 0xFF) : null;
        //*/
        return (_validate_get(col_num, "ftInt")) ? colStorage.getDataColumns(col_num).getInt(row_counter * 4) : null;
        //return (null_balls[col_num].get() == 0) ? data_columns[col_num].getInt() : null;
    }

    @Override
    public Long get_long(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);

        String type = colMetadata.getType(col_num);
        if (type.equals("ftInt"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (long)colStorage.getDataColumns(col_num).getInt(row_counter * 4) : null;
        else if (type.equals("ftShort"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (long)colStorage.getDataColumns(col_num).getShort(row_counter * 2) : null;
        else if (type.equals("ftUByte"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (long)(colStorage.getDataColumns(col_num).get(row_counter) & 0xFF) : null;

        return (_validate_get(col_num, "ftLong")) ? colStorage.getDataColumns(col_num).getLong(row_counter * 8) : null;
    }

    @Override
    public Float get_float(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);

        String type = colMetadata.getType(col_num);
        if (type.equals("ftInt"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (float)colStorage.getDataColumns(col_num).getInt(row_counter * 4) : null;
        else if (type.equals("ftShort"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (float)colStorage.getDataColumns(col_num).getShort(row_counter * 2) : null;
        else if (type.equals("ftUByte"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (float)(colStorage.getDataColumns(col_num).get(row_counter) & 0xFF) : null;

        return (_validate_get(col_num, "ftFloat")) ? colStorage.getDataColumns(col_num).getFloat(row_counter * 4) : null;
    }

    @Override
    public Double get_double(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);

        String type = colMetadata.getType(col_num);
        if (type.equals("ftFloat"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (double)colStorage.getDataColumns(col_num).getFloat(row_counter * 4) : null;
        else if (type.equals("ftLong"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (double)colStorage.getDataColumns(col_num).getLong(row_counter * 8) : null;
        else if (type.equals("ftInt"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (double)colStorage.getDataColumns(col_num).getInt(row_counter * 4) : null;
        else if (type.equals("ftShort"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (double)colStorage.getDataColumns(col_num).getShort(row_counter * 2) : null;
        else if (type.equals("ftUByte"))
            return colStorage.isValueNotNull(col_num, row_counter) ? (double)(colStorage.getDataColumns(col_num).get(row_counter) & 0xFF) : null;


        return (_validate_get(col_num, "ftDouble")) ? colStorage.getDataColumns(col_num).getDouble(row_counter * 8) : null;
    }

    @Override
    public String get_varchar(int col_num) throws ConnException, UnsupportedEncodingException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Get bytes the size of the varchar column into string_bytes
        if (col_calls[col_num]++ > 0) {
            // Resetting buffer position in case someone runs the same get()
            colStorage.getDataColumns(col_num).position(colStorage.getDataColumns(col_num).position() - colMetadata.getSize(col_num));
        }
        colStorage.getDataColumns(col_num).get(string_bytes, 0, colMetadata.getSize(col_num));

        return (_validate_get(col_num, "ftVarchar")) ? ("X" + (new String(string_bytes, 0, colMetadata.getSize(col_num), varchar_encoding))).trim().substring(1) : null;
    }

    @Override
    public String get_nvarchar(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        int nvarc_len = colStorage.getNvarcLenColumn(col_num).getInt(row_counter * 4);

        // Get bytes the size of this specific nvarchar into string_bytes
        if (col_calls[col_num]++ > 0)
            colStorage.getDataColumns(col_num).position(colStorage.getDataColumns(col_num).position() - nvarc_len);
        colStorage.getDataColumns(col_num).get(string_bytes, 0, nvarc_len);

        return (_validate_get(col_num, "ftBlob")) ? new String(string_bytes, 0, nvarc_len, UTF8) : null;
    }

    @Override
    public Date get_date(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);

        return (_validate_get(col_num, "ftDate")) ? intToDate(colStorage.getDataColumns(col_num).getInt(4*row_counter), zone) : null;
    }

    @Override
    public Timestamp get_datetime(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        return (_validate_get(col_num, "ftDateTime")) ? longToDt(colStorage.getDataColumns(col_num).getLong(8* row_counter), zone) : null;
    }

    @Override
    public Date get_date(int col_num) throws ConnException {

        return get_date(col_num, SYSTEM_TZ); // system_tz, UTC
    }

    @Override
    public Timestamp get_datetime(int col_num) throws ConnException {   // set / get work with starting index 1

        return get_datetime(col_num, SYSTEM_TZ); // system_tz, UTC
    }

    // -o-o-o-o-o  By column name -o-o-o-o-o
    @Override
    public Boolean getBoolean(String col_name) throws ConnException {

        return getBoolean(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Byte get_ubyte(String col_name) throws ConnException {

        return get_ubyte(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Short get_short(String col_name) throws ConnException {

        return get_short(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Integer get_int(String col_name) throws ConnException {

        return get_int(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Long get_long(String col_name) throws ConnException {

        return get_long(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Float get_float(String col_name) throws ConnException {

        return get_float(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Double get_double(String col_name) throws ConnException {

        return get_double(colMetadata.getColNumByName(col_name));
    }

    @Override
    public String get_varchar(String col_name) throws ConnException, UnsupportedEncodingException {

        return get_varchar(colMetadata.getColNumByName(col_name));
    }

    @Override
    public String get_nvarchar(String col_name) throws ConnException {

        return get_nvarchar(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Date get_date(String col_name) throws ConnException {

        return get_date(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Date get_date(String col_name, ZoneId zone) throws ConnException {

        return get_date(colMetadata.getColNumByName(col_name), zone);
    }

    @Override
    public Timestamp get_datetime(String col_name) throws ConnException {

        return get_datetime(colMetadata.getColNumByName(col_name));
    }

    @Override
    public Timestamp get_datetime(String col_name, ZoneId zone) throws ConnException {

        return get_datetime(colMetadata.getColNumByName(col_name), zone);
    }

    // Sets
    // ----

    boolean _validate_set(int col_num, Object value, String value_type) throws ConnException {
        boolean is_null = false;
        // Validate type
        String type = colMetadata.getType(col_num);
        if (!type.equals(value_type))
            throw new ConnException("Trying to set " + value_type + " on a column number " + col_num + " of type " + type);

        // Optional null handling - if null is appropriate, mark null column
        if (value == null) {
            if (colStorage.getNullColumn(col_num) != null) {
                colStorage.getNullColumn(col_num).put((byte)1);
                is_null = true;
            } else {
                throw new ConnException("Trying to set null on a non nullable column of type " + type);
            }
        }
        return is_null;
    }

    @Override
    public boolean set_boolean(int col_num, Boolean value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).put((byte)(_validate_set(col_num, value, "ftBool") ? 0 : (value == true) ? 1 : 0));
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_ubyte(int col_num, Byte value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Check the byte is positive
        if (value!= null && value < 0 )
            throw new ConnException("Trying to set a negative byte value on an unsigned byte column");

        // Set actual value - null or positive at this point
        colStorage.getDataColumns(col_num).put(_validate_set(col_num, value, "ftUByte") ? 0 : value);

        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_short(int col_num, Short value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).putShort(_validate_set(col_num, value, "ftShort") ? 0 : value);

        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_int(int col_num, Integer value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).putInt(_validate_set(col_num, value, "ftInt") ? 0 : value);

        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_long(int col_num, Long value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).putLong(_validate_set(col_num, value, "ftLong") ? (long) 0 : value);

        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_float(int col_num, Float value) throws ConnException {   col_num--;  // set / get work with starting index 1
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).putFloat(_validate_set(col_num, value, "ftFloat") ? (float)0.0 : value);

        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_double(int col_num, Double value) throws ConnException {  col_num--;
        _validate_index(col_num);
        // Set actual value
        colStorage.getDataColumns(col_num).putDouble(_validate_set(col_num, value, "ftDouble") ? 0.0 : value);

        // Mark column as set
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_varchar(int col_num, String value) throws ConnException, UnsupportedEncodingException {  col_num--;
        _validate_index(col_num);
        // Set actual value - padding with spaces to the left if needed
        string_bytes = _validate_set(col_num, value, "ftVarchar") ? "".getBytes(varchar_encoding) : value.getBytes(varchar_encoding);
        int colSize = colMetadata.getSize(col_num);
        if (string_bytes.length > colSize)
            throw new ConnException("Trying to set string of size " + string_bytes.length + " on column of size " +  colSize);
        // Generate missing spaces to fill up to size
        byte [] spaces = new byte[colSize - string_bytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space

        // Set value and added spaces if needed
        colStorage.getDataColumns(col_num).put(string_bytes);
        colStorage.getDataColumns(col_num).put(spaces);
        // data_columns[col_num].put(String.format("%-" + col_sizes[col_num] + "s", value).getBytes());

        // Mark column as set
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_nvarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException {  col_num--;
        _validate_index(col_num);
        // Convert string to bytes
        string_bytes = _validate_set(col_num, value, "ftBlob") ? "".getBytes(UTF8) : value.getBytes(UTF8);

        // Add string length to lengths column
        colStorage.getNvarcLenColumn(col_num).putInt(string_bytes.length);

        // Set actual value
        if (string_bytes.length > colStorage.getDataColumns(col_num).remaining()) {
            ByteBuffer new_text_buf = ByteBuffer.allocateDirect((colStorage.getDataColumns(col_num).capacity() +
                    string_bytes.length) * 2).order(ByteOrder.LITTLE_ENDIAN);
            new_text_buf.put(colStorage.getDataColumns(col_num));
            colStorage.setDataColumns(col_num, new_text_buf);
        }
        colStorage.getDataColumns(col_num).put(string_bytes);

        // Mark column as set
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_date(int col_num, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
        _validate_index(col_num);

        // Set actual value
        colStorage.getDataColumns(col_num).putInt(_validate_set(col_num, date, "ftDate") ? 0 : dateToInt(date, zone));

        // Mark column as set
        columns_set.set(col_num);

        return true;
    }

    @Override
    public boolean set_datetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
        _validate_index(col_num);

        //ZonedDateTime dt = ts.toLocalDateTime().atZone(zone);
        // ZonedDateTime dt = ts.toInstant().atZone(zone);

        // Set actual value
        colStorage.getDataColumns(col_num).putLong(_validate_set(col_num, ts, "ftDateTime") ? 0 : dtToLong(ts, zone));

        // Mark column as set
        columns_set.set(col_num);

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

        return colMetadata.getName(_validate_col_num(col_num));
    }

    @Override
    public String get_col_type(int col_num) throws ConnException {
        return colMetadata.getType(_validate_col_num(col_num));
    }

    @Override
    public String get_col_type(String col_name) throws ConnException {
        Integer colNum = colMetadata.getColNumByName(col_name);
        if (colNum == null)
            throw new ConnException("\nno column found for name: " + col_name + "\nExisting columns: \n" + colMetadata.getAllNames());
        return get_col_type(colNum);
    }

    @Override
    public int get_col_size(int col_num) throws ConnException {

        return colMetadata.getSize(_validate_col_num(col_num));
    }

    @Override
    public boolean is_col_nullable(int col_num) throws ConnException {

        return colMetadata.isNullable(_validate_col_num(col_num));
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
