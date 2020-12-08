package com.sqream.jdbc.connector;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.fetchService.FetchService;
import com.sqream.jdbc.connector.fetchService.FetchServiceFactory;
import com.sqream.jdbc.connector.storage.*;
import com.sqream.jdbc.connector.storage.fetchStorage.EmptyFetchStorage;
import com.sqream.jdbc.connector.storage.fetchStorage.FetchStorage;
import com.sqream.jdbc.connector.storage.fetchStorage.FetchStorageImpl;
import com.sqream.jdbc.connector.serverAPI.SqreamConnection;
import com.sqream.jdbc.connector.serverAPI.SqreamConnectionFactory;
import com.sqream.jdbc.connector.serverAPI.Statement.SqreamExecutedStatement;

import java.math.BigDecimal;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
import static com.sqream.jdbc.utils.Utils.calcRowLimit;

public class ConnectorImpl implements Connector {
    private static final Logger LOGGER = Logger.getLogger(ConnectorImpl.class.getName());

    private static final String DEFAULT_CHARACTER_CODES = "ascii";
    // Date/Time conversion related
    private static final ZoneId SYSTEM_TZ = ZoneId.systemDefault();
    private static final Charset UTF8 = StandardCharsets.UTF_8;

    private static final int BYTE_BUFFER_POOL_SIZE = 3;

    private String varcharEncoding = DEFAULT_CHARACTER_CODES;  // default encoding/decoding for varchar columns

    private String database;
    private String user;
    private String password;
    private String service;
    private ConnectionParams connParams;

    // Binary data related
    public static final int ROWS_PER_FLUSH_LIMIT = 1_000_000;
    public static final int BYTES_PER_FLUSH_LIMIT = 200 * 1024 * 1024;

    private FetchStorage fetchStorage;
    private FlushStorage flushStorage;

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

    private SqreamConnection sqreamConnection;
    private SqreamExecutedStatement sqreamExecutedStatement;

    private int timeout = 0;
    private int insertBufferLimit = BYTES_PER_FLUSH_LIMIT;

    public ConnectorImpl(ConnectionParams connParams) throws ConnException {
        /* JSON parsing engine setup, initial socket connection */
        this.connParams = connParams;
        if (this.connParams.getInsertBuffer() != null && this.connParams.getInsertBuffer() > 0) {
            this.insertBufferLimit = this.connParams.getInsertBuffer();
        }
    }

    private int flush() {
        BlockDto blockForFlush = flushStorage.getBlock();
        int rowsFlush = blockForFlush.getFillSize();
        if (rowsFlush > 0) {
            flushService.process(blockForFlush, byteBufferPool);
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
        this.database = database;
        this.user = user;
        this.password = password;
        this.service = service;
        this.connParams = ConnectionParams.builder()
                .from(connParams)
                .dbName(database)
                .user(user)
                .password(password)
                .service(service)
                .build();
        this.sqreamConnection = SqreamConnectionFactory.openConnection(this.connParams);
        return sqreamConnection.getId();
    }

    @Override
    public int execute(String statement) throws ConnException {
        /* Retains behavior of original execute()  */

        int defaultChunksize = (int) Math.pow(10,6);
        if (timeout > 0) {
            return execute(statement, defaultChunksize, timeout);
        } else {
            return execute(statement, defaultChunksize);
        }
    }

    private int execute(String statement, int chunkSize, int timeout) throws ConnException {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<Integer> future = executor.submit(() -> execute(statement, chunkSize));
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new ConnException(e);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new ConnTimeoutException(e);
        }
    }

    @Override
    public int execute(String statement, int chunkSize) throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Statement=[{0}], ChunkSize=[{1}]", statement, chunkSize));

        if (chunkSize < 0) {
            throw new ConnException("chunk size should be positive, got " + chunkSize);
        }
        if (sqreamExecutedStatement != null && sqreamExecutedStatement.isOpen()) {
            try {
                sqreamExecutedStatement.close();
            } catch (Exception e) {
                throw new ConnException(e);
            }
        }

        this.sqreamExecutedStatement = this.sqreamConnection
                .createStatement()
                .prepare(statement)
                .execute();

        this.validator = new InsertValidator(sqreamExecutedStatement.getMeta());

        // First fetch on the house, auto close statement if no data returned
        if (sqreamExecutedStatement.getType().equals(QUERY)) {
            fetchService = FetchServiceFactory.getService(sqreamExecutedStatement, fetchSize);
            totalRowCounter = 0;
            fetchService.process(fetchLimit);
            BlockDto fetchedBlock = fetchService.getBlock();
            if (fetchedBlock != null) {
                fetchStorage = new FetchStorageImpl(sqreamExecutedStatement.getMeta(), fetchedBlock);
            } else {
                fetchStorage = new EmptyFetchStorage();
            }
            if (fetchService.isClosed()) {
                close();
            }
        } else if (sqreamExecutedStatement.getType().equals(NETWORK_INSERT)) {
            int rowsPerFlush = calcRowLimit(sqreamExecutedStatement.getMeta(), ROWS_PER_FLUSH_LIMIT, insertBufferLimit);
            BlockDto block = new MemoryAllocationService().buildBlock(sqreamExecutedStatement.getMeta(), rowsPerFlush);
            this.flushService = FlushService.getInstance(sqreamExecutedStatement);
            flushStorage = new FlushStorage(sqreamExecutedStatement.getMeta(), block, insertBufferLimit);

            byteBufferPool = createByteBufferPool(rowsPerFlush);
        }
        return sqreamExecutedStatement.getId();
    }

    @Override
    public boolean next() throws ConnException {
        if (sqreamExecutedStatement.getType().equals(NETWORK_INSERT)) {
            // Flush and clean if needed
            if (!flushStorage.next()) {
                flush();
            }
        } else if (sqreamExecutedStatement.getType().equals(QUERY)) {
        	if (fetchLimit !=0 && totalRowCounter == fetchLimit) {
                return false;  // MaxRow limit reached, stop even if more data was fetched
            }
        	if (!fetchStorage.next()) {
        	    BlockDto fetchedBlock = fetchService.getBlock();
        		if (fetchedBlock == null || fetchedBlock.getFillSize() == 0) {
        		    close();
                    return false; // No more data and we've read all we have
                }
                // Set new active buffer to be reading data from
                fetchStorage.setBlock(fetchedBlock);
            }
            totalRowCounter++;
        } else if (sqreamExecutedStatement.getType().equals(NON_QUERY)) {
            throw new ConnException("Calling next() on a non insert / select query");
        } else {
            throw new ConnException("Calling next() on a statement type different than INSERT / SELECT / DML: " + sqreamExecutedStatement.getType().getValue());
        }
        return true;
    }

    @Override
    public void close() throws ConnException {
        LOGGER.log(Level.FINE, MessageFormat.format("Close statement: openStatement=[{0}], statementType=[{1}]]",
                isOpenStatement(), sqreamExecutedStatement != null ? sqreamExecutedStatement.getType() : null));

    	if (isOpen()) {
    		if (sqreamExecutedStatement != null && sqreamExecutedStatement.isOpen()) {
    			if (NETWORK_INSERT.equals(sqreamExecutedStatement.getType())) {
    	            flush();
    	            flushService.close();
    	            if (byteBufferPool != null) {
                        byteBufferPool.close();
                    }
    	        }
                try {
                    this.sqreamExecutedStatement.close();
                } catch (Exception e) {
                    throw new ConnException(e);
                }
            }
        }
    }

    @Override
    public boolean closeConnection() throws ConnException {
        if (sqreamConnection != null && sqreamConnection.isOpen()) {
            if (sqreamExecutedStatement != null && sqreamExecutedStatement.isOpen()) { // Close open statement if exists
                close();
            }
            try {
                sqreamConnection.close();
            } catch (Exception e) {
                throw new ConnException(e);
            }
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
    public BigDecimal getBigDecimal(int colNum) {
        validator.validateColumnIndex(colNum - 1);
        return fetchStorage.getBigDecimal(colNum - 1);
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

        return getBoolean(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Byte getUbyte(String colName) {

        return getUbyte(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Short getShort(String colName) {

        return getShort(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Integer getInt(String colName) {

        return getInt(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Long getLong(String colName) {

        return getLong(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Float getFloat(String colName) {

        return getFloat(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Double getDouble(String colName) {

        return getDouble(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public String getVarchar(String colName) {

        return getVarchar(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public String getNvarchar(String colName) {

        return getNvarchar(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Date getDate(String colName) {

        return getDate(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Date getDate(String colName, ZoneId zone) {

        return getDate(sqreamExecutedStatement.getMeta().getColNumByName(colName), zone);
    }

    @Override
    public Timestamp getDatetime(String colName) {

        return getDatetime(sqreamExecutedStatement.getMeta().getColNumByName(colName));
    }

    @Override
    public Timestamp getDatetime(String colName, ZoneId zone) {

        return getDatetime(sqreamExecutedStatement.getMeta().getColNumByName(colName), zone);
    }

    @Override
    public int getTimeout() {
        return this.timeout;
    }

    // Sets
    // ----

    @Override
    public boolean setBoolean(int colNum, Boolean value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setBoolean(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setUbyte(int colNum, Byte value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setUbyte(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setShort(int colNum, Short value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setShort(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setInt(int colNum, Integer value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setInt(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setLong(int colNum, Long value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setLong(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setFloat(int colNum, Float value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setFloat(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setDouble(int colNum, Double value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setDouble(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setBigDecimal(int colNum, BigDecimal value) {
        validator.validateSet(colNum - 1, value);
        flushStorage.setBigDecimal(colNum - 1, value);
        return true;
    }

    @Override
    public boolean setVarchar(int colNum, String value) throws ConnException {
        try {
            validator.validateSet(colNum - 1, value);
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
    public boolean setNvarchar(int colNum, String value) throws ConnException {
        validator.validateSet(colNum - 1, value);
        // Convert string to bytes
        byte[] stringBytes = value == null ? "".getBytes(UTF8) : value.getBytes(UTF8);
        flushStorage.setNvarchar(colNum - 1, stringBytes, value);
        return true;
    }

    @Override
    public boolean setDate(int colNum, Date date, ZoneId zone) {
        validator.validateSet(colNum - 1, date);
        flushStorage.setDate(colNum - 1, date, zone);
        return true;
    }

    @Override
    public boolean setDatetime(int colNum, Timestamp ts, ZoneId zone) {
        validator.validateSet(colNum - 1, ts);
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

    @Override
    public void setTimeout(int seconds) {
        this.timeout = seconds;
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
        return sqreamExecutedStatement.getId();
    }

    @Override
    public String getQueryType() {
        return sqreamExecutedStatement.getType().getValue();
    }

    @Override
    public int getRowLength() {  // number of columns for this query

        return sqreamExecutedStatement.getMeta().getRowLength();
    }

    @Override
    public String getColName(int colNum) throws ConnException {

        return sqreamExecutedStatement.getMeta().getName(validateColNum(colNum));
    }

    @Override
    public String getColType(int colNum) throws ConnException {
        return sqreamExecutedStatement.getMeta().getType(validateColNum(colNum));
    }

    @Override
    public String getColType(String colName) throws ConnException {
        Integer colNum = sqreamExecutedStatement.getMeta().getColNumByName(colName);
        if (colNum == null)
            throw new ConnException("\nno column found for name: " + colName + "\nExisting columns: \n" + sqreamExecutedStatement.getMeta().getAllNames());
        return getColType(colNum);
    }

    @Override
    public int getColSize(int colNum) throws ConnException {

        return sqreamExecutedStatement.getMeta().getSize(validateColNum(colNum));
    }

    @Override
    public boolean isColNullable(int colNum) throws ConnException {

        return sqreamExecutedStatement.getMeta().isNullable(validateColNum(colNum));
    }

    @Override
    public boolean isOpenStatement() {
        return this.sqreamExecutedStatement != null && this.sqreamExecutedStatement.isOpen();
    }

    @Override
    public boolean isOpen() {
        return this.sqreamConnection != null && this.sqreamConnection.isOpen();
    }

    @Override
    public AtomicBoolean checkCancelStatement() {
        return this.IsCancelStatement;
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

    private ByteBufferPool createByteBufferPool(int blockSize) throws ConnException {
        try {
            return new ByteBufferPool(BYTE_BUFFER_POOL_SIZE, blockSize, sqreamExecutedStatement.getMeta());
        } catch (OutOfMemoryError error) {
            try {
                this.closeConnection();
                long maxMemory = Runtime.getRuntime().maxMemory() / (1024 * 1024);
                LOGGER.log(Level.FINE, MessageFormat.format("Not enough heap memory [{0} Mb] to process", maxMemory));
                throw new OutOfMemoryError(MessageFormat.format("Not enough heap memory [{0} Mb] to process", maxMemory));
            } catch (ConnException e) {
                throw new ConnException(e);
            }
        }
    }
}
