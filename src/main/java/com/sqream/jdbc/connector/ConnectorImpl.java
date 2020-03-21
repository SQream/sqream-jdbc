package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.enums.StatementType;
import com.sqream.jdbc.connector.fetchService.FetchService;
import com.sqream.jdbc.connector.fetchService.FetchServiceFactory;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import com.sqream.jdbc.connector.storage.*;
import com.sqream.jdbc.connector.storage.fetchStorage.EmptyFetchStorage;
import com.sqream.jdbc.connector.storage.fetchStorage.FetchStorage;
import com.sqream.jdbc.connector.storage.fetchStorage.FetchStorageImpl;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.List;
import java.text.MessageFormat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;

import java.io.UnsupportedEncodingException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.sqream.jdbc.connector.enums.StatementType.*;

public class ConnectorImpl implements Connector {
    private static final Logger LOGGER = Logger.getLogger(ConnectorImpl.class.getName());

    private static final String DEFAULT_CHARACTER_CODES = "ascii";
    // Date/Time conversion related
    private static final ZoneId SYSTEM_TZ = ZoneId.systemDefault();
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private static final int BYTE_BUFFER_POOL_SIZE = 3;

    private SQSocketConnector socket;
    private Messenger messenger;

    private int connectionId = -1;
    private int statementId = -1;
    private String varcharEncoding = DEFAULT_CHARACTER_CODES;  // default encoding/decoding for varchar columns

    private String database;
    private String user;
    private String password;
    private String service;
    private boolean useSsl;

    // Binary data related
    public static final int ROWS_PER_FLUSH = 100_000;

    // Column metadata
    private StatementType statementType;
    private int rowLength;

    private TableMetadata tableMetadata;
    private FetchStorage fetchStorage;
    private FlushStorage flushStorage;

    private boolean openStatement = false;

    // Column Storage
    private int fetchLimit = 0;
    private int fetchSize = 0; // 0 means no limit.

    // Get / Set related
    private int totalRowCounter;

    // Managing stop_statement
    private AtomicBoolean IsCancelStatement = new AtomicBoolean(false);

    private InsertValidator validator;
    private FlushService flushService;
    private FetchService fetchService;
    private ByteBufferPool byteBufferPool;

    public ConnectorImpl(String ip, int port, boolean cluster, boolean ssl) throws ConnException {
        /* JSON parsing engine setup, initial socket connection */
        useSsl = ssl;
        socket = SQSocketConnector.connect(ip, port, useSsl, cluster);
        this.messenger = MessengerImpl.getInstance(socket);
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
    private void parseQueryType(List<ColumnMetadataDto> columnsMetadata) {

        rowLength = columnsMetadata.size();
        if(rowLength ==0) {
            return;
        }

        tableMetadata = TableMetadata.builder()
                .rowLength(rowLength)
                .fromColumnsMetadata(columnsMetadata)
                .statementType(statementType)
                .build();

        validator = new InsertValidator(tableMetadata);

        // Create Storage for insert / select operations
        if (statementType.equals(INSERT)) {
            // Initiate buffers for each column using the metadata
            BlockDto block = new MemoryAllocationService().buildBlock(tableMetadata, ROWS_PER_FLUSH);
            flushService = FlushService.getInstance(socket, messenger);
            flushStorage = new FlushStorage(tableMetadata, block);

            byteBufferPool = new ByteBufferPool(BYTE_BUFFER_POOL_SIZE, ROWS_PER_FLUSH, tableMetadata);
        }
    }

    private int flush() {
        BlockDto blockForFlush = flushStorage.getBlock();
        int rowsFlush = blockForFlush.getFillSize();
        if (rowsFlush > 0) {
            flushService.process(tableMetadata, blockForFlush, byteBufferPool);
            flushStorage.setBlock(byteBufferPool.getBlock());
        }
        return rowsFlush;
    }

    // User API Functions
    /* ------------------
     * connect(), execute(), next(), close(), close_conenction()
     *
     */
    @Override
    public int connect(String database, String user, String password, String service) throws ConnException {
        //"'{'\"username\":\"{0}\", \"password\":\"{1}\", \"connectDatabase\":\"{2}\", \"service\":\"{3}\"'}'";

        this.database = database;
        this.user = user;
        this.password = password;
        this.service = service;

        ConnectionStateDto connState = messenger.connect(this.database, this.user, this.password, this.service);
        connectionId = connState.getConnectionId();
        varcharEncoding = connState.getVarcharEncoding();

        return connectionId;
    }

    @Override
    public int execute(String statement) throws ConnException {
        /* Retains behavior of original execute()  */

        int defaultChunksize = (int) Math.pow(10,6);
        return execute(statement, defaultChunksize);
    }

    @Override
    public int execute(String statement, int chunkSize) throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Statement=[{0}], ChunkSize=[{1}]", statement, chunkSize));

        if (chunkSize < 0) {
            throw new ConnException("chunk size should be positive, got " + chunkSize);
        }
        if (openStatement) {
            // Automatically close previous unclosed statement
            close();
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
            messenger.reconnect(database, user, password, service, connectionId, statementState.getListenerId());
            messenger.isStatementReconstructed(statementId);
        }

        // Getting query type manouver and setting the type of query
        messenger.execute();
        List<ColumnMetadataDto> queryType = messenger.queryTypeInput();

        if (queryType.isEmpty()) {
            queryType = messenger.queryTypeOut();
            statementType = queryType.isEmpty() ? DML : SELECT;
        }
        else {
            statementType = INSERT;
        }

        // Select or Insert statement - parse queryType response for metadata
        if (!statementType.equals(DML)) {
            parseQueryType(queryType);
        }
        // First fetch on the house, auto close statement if no data returned
        if (statementType.equals(SELECT)) {
            fetchService = FetchServiceFactory.getService(socket, messenger, tableMetadata, fetchSize);
            totalRowCounter = 0;
            fetchService.process(fetchLimit);
            BlockDto fetchedBlock = fetchService.getBlock();
            if (fetchedBlock != null) {
                fetchStorage = new FetchStorageImpl(tableMetadata, fetchedBlock);
            } else {
                fetchStorage = new EmptyFetchStorage();
            }
            if (fetchService.isClosed()) {
                close();
            }
        }
        return statementId;
    }

    @Override
    public boolean next() throws ConnException {
        if (statementType.equals(INSERT)) {
            // Flush and clean if needed
            if (!flushStorage.next()) {
                flush();
            }
        } else if (statementType.equals(SELECT)) {
        	if (fetchLimit !=0 && totalRowCounter == fetchLimit) {
                return false;  // MaxRow limit reached, stop even if more data was fetched
            }
        	if (!fetchStorage.next()) {
        	    BlockDto fetchedBlock = fetchService.getBlock();
        		if (fetchedBlock == null) {
        		    close();
                    return false; // No more data and we've read all we have
                }
                // Set new active buffer to be reading data from
                fetchStorage.setBlock(fetchedBlock);
            }
            totalRowCounter++;
        } else if (statementType.equals(DML)) {
            throw new ConnException("Calling next() on a non insert / select query");
        } else {
            throw new ConnException("Calling next() on a statement type different than INSERT / SELECT / DML: " + statementType.getValue());
        }
        return true;
    }

    @Override
    public void close() throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Close statement: openStatement=[{0}], statementType=[{1}]]", openStatement, statementType));

    	if (isOpen()) {
    		if (openStatement) {
    			if (statementType != null && statementType.equals(INSERT)) {
    	            flush();
    	            flushService.close();
    	        }
    	            // Statement is finished so no need to reset row_counter etc
                messenger.closeStatement();
                openStatement = false;  // set to true in execute()
            }
        }
    }

    @Override
    public boolean closeConnection() throws ConnException {
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
    public Boolean getBoolean(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getBoolean(colNum - 1);
    }

    @Override
    public Byte getUbyte(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getUbyte(colNum - 1);
    }

    @Override
    public Short getShort(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getShort(colNum - 1);
    }

    @Override
    public Integer getInt(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getInt(colNum - 1);
    }

    @Override
    public Long getLong(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getLong(colNum - 1);
    }

    @Override
    public Float getFloat(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getFloat(colNum - 1);
    }

    @Override
    public Double getDouble(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getDouble(colNum - 1);
    }

    @Override
    public String getVarchar(int colNum) {
        int colIndex = colNum - 1;
        validator.validateColumnIndex(colIndex);
        return fetchStorage.getVarchar(colIndex, varcharEncoding);
    }

    @Override
    public String getNvarchar(int colNum) {
        int colIndex = colNum - 1;
        validator.validateColumnIndex(colIndex);
        return fetchStorage.getNvarchar(colIndex, UTF8);
    }

    @Override
    public Date getDate(int colNum, ZoneId zone) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getDate(colNum - 1, zone);
    }

    @Override
    public Timestamp getDatetime(int colNum, ZoneId zone) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getTimestamp(colNum - 1, zone);
    }

    @Override
    public Date getDate(int colNum) {
        return getDate(colNum, SYSTEM_TZ); // system_tz, UTC
    }

    @Override
    public Timestamp getDatetime(int colNum) {
        return getDatetime(colNum, SYSTEM_TZ); // system_tz, UTC
    }

    // -o-o-o-o-o  By column name -o-o-o-o-o
    @Override
    public Boolean getBoolean(String colName) {

        return getBoolean(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Byte getUbyte(String colName) {

        return getUbyte(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Short getShort(String colName) {

        return getShort(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Integer getInt(String colName) {

        return getInt(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Long getLong(String colName) {

        return getLong(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Float getFloat(String colName) {

        return getFloat(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Double getDouble(String colName) {

        return getDouble(tableMetadata.getColNumByName(colName));
    }

    @Override
    public String getVarchar(String colName) {

        return getVarchar(tableMetadata.getColNumByName(colName));
    }

    @Override
    public String getNvarchar(String colName) {

        return getNvarchar(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Date getDate(String colName) {

        return getDate(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Date getDate(String colName, ZoneId zone) {

        return getDate(tableMetadata.getColNumByName(colName), zone);
    }

    @Override
    public Timestamp getDatetime(String colName) {

        return getDatetime(tableMetadata.getColNumByName(colName));
    }

    @Override
    public Timestamp getDatetime(String colName, ZoneId zone) {

        return getDatetime(tableMetadata.getColNumByName(colName), zone);
    }

    // Sets
    // ----

    @Override
    public boolean setBoolean(int colNum, Boolean value) {
        validator.validateSet(colNum - 1, value, "ftBool");
        flushStorage.setBoolean(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setUbyte(int colNum, Byte value) {
        return setUbyte(colNum, value, true);
    }

    private boolean setUbyte(int colNum, Byte value, boolean validateValue) {
        validator.validateSet(colNum - 1, value, "ftUByte");
        if (validateValue) {
            validator.validateUbyte(value);
        }
        flushStorage.setUbyte(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setShort(int colNum, Short value) {
        if ("ftUByte".equals(tableMetadata.getType(colNum - 1))) {
            if (value < 0 || value > 255) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "Trying to set wrong value [{0}] on an unsigned byte column", value));
            }
            setUbyte(colNum, value.byteValue(), false);
        } else {
            validator.validateSet(colNum - 1, value, "ftShort");
            flushStorage.setShort(colNum - 1, value);
        }

        return true;
    }

    @Override
    public boolean setInt(int colNum, Integer value) {
        validator.validateSet(colNum - 1, value, "ftInt");
        flushStorage.setInt(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setLong(int colNum, Long value) {
        validator.validateSet(colNum - 1, value, "ftLong");
        flushStorage.setLong(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setFloat(int colNum, Float value) {
        validator.validateSet(colNum - 1, value, "ftFloat");
        flushStorage.setFloat(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setDouble(int colNum, Double value) {
        validator.validateSet(colNum - 1, value, "ftDouble");
        flushStorage.setDouble(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setVarchar(int colNum, String value) throws ConnException {
        try {
            validator.validateSet(colNum - 1, value, "ftVarchar");
            // converting to byte array before validation
            byte[] stringBytes = value == null ? "".getBytes(varcharEncoding) : value.getBytes(varcharEncoding);
            validator.validateVarchar(colNum - 1, stringBytes.length);
            flushStorage.setVarchar(colNum - 1, stringBytes, value);
            return true;
        } catch (UnsupportedEncodingException e) {
            throw new ConnException(e);
        }
    }

    @Override
    public boolean setNvarchar(int colNum, String value) {
        validator.validateSet(colNum - 1, value, "ftBlob");
        // Convert string to bytes
        byte[] stringBytes = value == null ? "".getBytes(UTF8) : value.getBytes(UTF8);
        flushStorage.setNvarchar(colNum - 1, stringBytes, value);
        return true;
    }

    @Override
    public boolean setDate(int colNum, Date date, ZoneId zone) {
        validator.validateSet(colNum - 1, date, "ftDate");
        flushStorage.setDate(colNum - 1, date, zone);
        return true;
    }

    @Override
    public boolean setDatetime(int colNum, Timestamp ts, ZoneId zone) {
        validator.validateSet(colNum - 1, ts, "ftDateTime");
        flushStorage.setDatetime(colNum - 1, ts, zone);
        return true;
    }

    @Override
    public boolean setDate(int colNum, Date value) {
        return setDate(colNum, value, SYSTEM_TZ); // system_tz, UTC
    }

    @Override
    public boolean setDatetime(int colNum, Timestamp value) {
        return setDatetime(colNum, value, SYSTEM_TZ); // system_tz, UTC
    }

    // Metadata
    // --------

    private int validateColNum(int colNum) throws ConnException {

        if (colNum <1)
            throw new ConnException ("Using a metadata function with a non positive column value");

        return --colNum;
    }

    @Override
    public int getStatementId() {
        return statementId;
    }

    @Override
    public String getQueryType() {
        return statementType.getValue();
    }

    @Override
    public int getRowLength() {  // number of columns for this query

        return rowLength;
    }

    @Override
    public String getColName(int colNum) throws ConnException {

        return tableMetadata.getName(validateColNum(colNum));
    }

    @Override
    public String getColType(int colNum) throws ConnException {
        return tableMetadata.getType(validateColNum(colNum));
    }

    @Override
    public String getColType(String colName) throws ConnException {
        Integer colNum = tableMetadata.getColNumByName(colName);
        if (colNum == null)
            throw new ConnException("\nno column found for name: " + colName + "\nExisting columns: \n" + tableMetadata.getAllNames());
        return getColType(colNum);
    }

    @Override
    public int getColSize(int colNum) throws ConnException {

        return tableMetadata.getSize(validateColNum(colNum));
    }

    @Override
    public boolean isColNullable(int colNum) throws ConnException {

        return tableMetadata.isNullable(validateColNum(colNum));
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
    public boolean setFetchLimit(int fetchLimit) throws ConnException{
        if (fetchLimit < 0) {
            throw new ConnException(MessageFormat.format(
                    "Max rows [{0}] to fetch should be non negative", fetchLimit));
        }
        this.fetchLimit = fetchLimit;
        return true;
    }

    @Override
    public int getFetchLimit() {
        return fetchLimit;
    }

    @Override
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }
}
