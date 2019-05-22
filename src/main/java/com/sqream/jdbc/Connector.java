package com.sqream.jdbc;

//Packing and unpacking columnar data
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

// Socket communication
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;
import java.io.InputStream;
import java.io.OutputStream;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.SSLSocket;


import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

// More SSL shite
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

// JSON parsing library
import com.eclipsesource.json.ParseException;
import com.eclipsesource.json.WriterConfig;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

import javax.script.Bindings;
import javax.script.ScriptContext;
//Formatting JSON strings, parsing JSONS
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import jdk.nashorn.internal.runtime.JSONListAdapter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.HashMap;
import java.text.MessageFormat;

// Datatypes for building columns and other
//import java.lang.reflect.Array;
import java.util.BitSet;
//import java.util.List;

// Unicode related
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

// Date / Time related
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
// Aux
import java.util.Arrays;   //  To allow debug prints via Arrays.toString
import java.util.stream.IntStream;

//Exceptions
import javax.script.ScriptException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import javax.net.ssl.SSLException;

// SSL over SocketChannel abstraction
import tlschannel.TlsChannel;
import tlschannel.ClientTlsChannel;

/* SQream Native Java Connector
   Version 3.0.0
  
  Usage Example:
 
  // Starting a connector instance and connecting to SQream
  Connector conn = new Connector("127.0.0.1", 3109, true, true);
  conn.connect("master", "sqream", "sqream", "sqream");
  
  // Creating a table
  stmt = "create or replace table test (ints int, words nvarchar(10))";
  conn.execute(stmt);
  conn.close();
        
  // Inserting some data
  int amount = (int)Math.pow(10, 7);
  long start = time();
  conn.execute("insert into shoko values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
  for (int i=0; i < 10; i++) {
      conn.set_int(1, 140000);
      conn.set_nvarchar(2, "שוקו");
      conn.next();
  }
    
  // Running a query on the data
  stmt = "select count (*) from test";
  conn.execute(stmt);
  while(conn.next()) 
      print("row count: " + conn.get_long(1));
  conn.close();
  
  // Retrieving data
  String stmt = "select top 3 * from test";
  conn.execute(stmt);
  while(conn.next()) {
     print("int received: " + conn.get_int(1));
     print("nvarchar received: " + conn.get_nvarchar(2));
  }
  conn.close();
  
  // Closing the connector
  conn.close_connection();
 
 */


public class Connector {

    // Class variables
    // ---------------
        
    // Protocol related
    byte protocol_version = 6;
    byte is_text;  // Catching the 2nd byte of a response
    long response_length;
    int connection_id = -1;
    int statement_id = -1;
    static Charset UTF8 = StandardCharsets.UTF_8;
    
    // Connection related
    SocketChannel s = SocketChannel.open();
    SSLContext ssl_context = SSLContext.getDefault();
    TlsChannel ss;  // secure socket
    String ip;
    int port;
    String database;
    String user = "sqream";
    String password = "sqream";
    String service = "sqream";
    boolean use_ssl;
    
    // Reconnecting parameters that don't appear before that stage
    int listener_id;
    int port_ssl;
    boolean reconnect;
    
    // JSON parsing related
    ScriptEngine engine;
    Bindings engine_bindings;
    ScriptObjectMirror json;
    String json_wrapper = "Java.asJSONCompatible({0})";
    //@SuppressWarnings("rawtypes") // Remove "Map is a raw type"  warning
    //https://stackoverflow.com/questions/2770321/what-is-a-raw-type-and-why-shouldnt-we-use-it
    Map<String, Object> response_json, col_data;
    JSONListAdapter query_type; // JSONListAdapter represents a list inside a JSON
    JSONListAdapter col_type_data; 
    JSONListAdapter fetch_sizes;
    Map<String, String> prepare_map;
    
    // Message sending related
    ByteBuffer message_buffer;
    ByteBuffer response_buffer = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer header = ByteBuffer.allocateDirect(10).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer response_message;
    int message_length, bytes_read;
    String response_string;
    boolean fetch_msg = false;
    int written;
    
    // Binary data related
    int FLUSH_SIZE = 10 * (int) Math.pow(10, 6);
    //byte [] buffer = new byte[FLUSH_SIZE];
    int row_size, rows_per_flush;
    int max_string_len = 0;   // For preallocating a bytearray for getVarchar/Nvarchar
    
    // Column metadata
    String statement_type;
    int row_length;
    String [] col_names;
    HashMap<String, Integer> col_names_map;
    String [] col_types;
    int []    col_sizes;
    BitSet col_nullable;
    BitSet col_tvc;
    boolean open_statement = false;

    // Column Storage
    ByteBuffer[] data_columns;
    //byte[][] null_columns;
    ByteBuffer[] null_columns;
    ByteBuffer null_resetter;
    ByteBuffer[] nvarc_len_columns;
    ByteBuffer [] null_balls;
    
    // Get / Set related
    int row_counter;
    BitSet columns_set;
    int total_bytes;
    boolean is_null;
    byte[] message_bytes;
    byte[] string_bytes; // Storing converted string to be set
    ByteBuffer[] fetch_buffers;
    ByteBuffer[] null_buffers;
    int new_rows_fetched, total_rows_fetched;
    byte [] spaces; 
    int nvarc_len;
    int col_num;
    int[] col_calls;
    
    // Date/Time conversion related
    static ZoneId UTC = ZoneOffset.UTC;
    static ZoneId system_tz = ZoneId.systemDefault();
    static int year, month, day, hour, minutes, seconds, ms;
    static int date_as_int, time_as_int;
    static long dt_as_long;
    static LocalDate local_date;
    static LocalDateTime local_datetime;

    // Managing stop_statement
    AtomicBoolean IsCancelStatement = new AtomicBoolean(false);
    
    
    // Communication Strings
    // ---------------------

    String connectDatabase = "'{'\"connectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\"'}'";
    String prepareStatement = "'{'\"prepareStatement\":\"{0}\", \"chunkSize\":{1}'}'";
    String reconnectDatabase = "'{'\"reconnectDatabase\":\"{0}\", \"username\":\"{1}\", \"password\":\"{2}\", \"service\":\"{3}\", \"connectionId\":{4, number, #}, \"listenerId\":{5, number, #}'}'";
    String reconstructStatement = "'{'\"reconstructStatement\":{0, number, #}'}'";
    String put = "'{'\"put\":{0, number, #}'}'";
    
    // Constant message template
    String simpleMessage = "'{'\"{0}\":\"{0}\"'}'"; 
    
    
    // General Helper Functions
    // ------------------------
    
    String form_json(String command) {  
        return MessageFormat.format(simpleMessage, command);
    }
    
    static void print(Object printable) {
        System.out.println(printable);
    }
    
    static void printbuf(ByteBuffer to_print, String description) {
        System.out.println(description + " : " + to_print);
    }
    
    static String decode(ByteBuffer message) {
        return UTF8.decode(message).toString();
    }
    
    static long time() {
        return System.currentTimeMillis();
    }
    
    static Date date_from_tuple(int year, int month, int day) {
        
        return Date.valueOf(LocalDate.of(year, month, day));
    }
    
    static Timestamp datetime_from_tuple(int year, int month, int day, int hour, int minutes, int seconds, int ms) {
            
        return Timestamp.valueOf(LocalDateTime.of(LocalDate.of(year, month, day), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6))));
    }

    
    static int date_to_int(Date d ,ZoneId zone) {
        
        if (local_date == null) 
            return 0;
        
        //ZonedDateTime zoned_date = d.toInstant().atZone(zone);
        
        LocalDate date = d.toLocalDate();
        year  = date.getYear();
        month = date.getMonthValue();
        day   = date.getDayOfMonth();

        month = (month + 9) % 12;
        year = year - month / 10;

        date_as_int = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));

    
        return date_as_int;
    }
    
    
    static long dt_to_long(Timestamp ts, ZoneId zone) {  // ZonedDateTime
        
        if (ts == null) 
            return 0;
        
        LocalDateTime datetime = ts.toInstant().atZone(zone).toLocalDateTime(); 
        
        //LocalDateTime datetime = ts.toLocalDateTime(); 
        year  = datetime.getYear();
        month = datetime.getMonthValue();
        day   = datetime.getDayOfMonth();
        
        month = (month + 9) % 12;
        year = year - month / 10;

        date_as_int = (365 * year + year / 4 - year / 100 + year / 400 + (month * 306 + 5) / 10 + (day - 1));
    
        time_as_int =  datetime.getHour() * 3600000;
        time_as_int += datetime.getMinute() * 60000;
        time_as_int += datetime.getSecond() * 1000;
        time_as_int += datetime.getNano() / 1000000;
        
        
        return (((long) date_as_int) << 32) | (time_as_int & 0xffffffffL);
    }
    
    
    static LocalDate _int_to_local_date(int date_as_int) {
        
        long yy = ((long)10000*date_as_int + 14780)/3652425;
        long ddd =  (date_as_int - (365*yy + yy/4 - yy/100 + yy/400));

        if (ddd < 0) 
        {
            yy -=  1;
            ddd = (date_as_int - (365*yy + yy/4 - yy/100 + yy/400));
        }
 
        long mi = (long)(100*ddd + 52)/3060;
        
        year = (int) (yy + (mi + 2)/12);
        month = (int)((mi + 2)%12) + 1;
        day = (int) (ddd - (mi*306 + 5)/10 + 1);
        
        
        return LocalDate.of(year, month, day);                  
    }
    
    
    static Date int_to_date(int date_as_int, ZoneId zone) {
        
        return Date.valueOf(_int_to_local_date(date_as_int));                   
    }
    

    static Timestamp long_to_dt(long dt_as_long, ZoneId zone) {
        
        date_as_int = (int)(dt_as_long >> 32);
        time_as_int = (int)dt_as_long;       
       
        // Get hour, minutes and seconds from the time part
        hour = time_as_int / 1000 / 60 / 60;
        minutes = (time_as_int / 1000 / 60) % 60 ;
        seconds = ((time_as_int) / 1000) % 60;
        ms = time_as_int % 1000;
        LocalDateTime local_dt = LocalDateTime.of(_int_to_local_date(date_as_int), LocalTime.of(hour, minutes, seconds, ms*(int)Math.pow(10, 6)));
        
        return Timestamp.valueOf(local_dt);
        //return Timestamp.from(local_dt.atZone(zone).toInstant());

    }
    
    
    // Aux Classes
    // -----------
    
    public class ConnException extends Exception {
        /*  Connector exception class */
        
        private static final long serialVersionUID = 1L;
        public ConnException(String message) {
            super(message);
        }
    }
    
    // Logging
    // -------
    
    boolean logging = true;
	boolean log(String line, String log_path) throws SQLException { 
		if (!logging)
			return true;
		
		try {
			Files.write(Paths.get(log_path), Arrays.asList(new String[] {line}), UTF_8, CREATE, APPEND);
		} catch (IOException e) {
			e.printStackTrace();
			throw new SQLException ("Error writing to SQDriver log");
		}
		
		return true;
	}
    
    
    /*//Experimental Sock class, not in use 
    class Sock {
        // Uses a SocketChannel for non SSL connections, SSLSocket for SSL 
        // Interface is that of a SocketChannel - using ByteBuffers      
        
        
        boolean use_ssl;
        SocketChannel sc = SocketChannel.open();
        SSLSocketFactory sslsocketfactory = (SSLSocketFactory) SSLSocketFactory.getDefault();
        SSLSocket ssl_socket;
        InputStream in;
        OutputStream out;
        int bytes_read;
        byte[] message_bytes;
        byte[] response_bytes;
        
        
        Sock(boolean use_ssl) throws IOException, NoSuchAlgorithmException{ //, KeyManagementException {
            
            this.use_ssl = use_ssl;
        }
        
        
        boolean connect(InetSocketAddress address) throws IOException, NoSuchAlgorithmException  {
            
            return connect(address.getHostName(), address.getPort());
        }

        
        boolean connect (String ip, int port) throws IOException, NoSuchAlgorithmException {
            
            if (use_ssl) {
                //SSLContext ssl_context = SSLContext.getDefault();
                ssl_socket = (SSLSocket) sslsocketfactory.createSocket(ip, port);
                in = ssl_socket.getInputStream();
                out = ssl_socket.getOutputStream();
            }
            else 
                sc.connect(new InetSocketAddress(ip, port));
                
            return true ;
        }
        
        
        int read(ByteBuffer response)  throws IOException{
            
            if (use_ssl) {
                response_bytes = new byte[response.limit()];
                bytes_read = in.read(response_bytes);
                response.clear();
                response.put(response_bytes);
            }
            
            return (use_ssl) ? bytes_read : sc.read(response);
        }
        
        
        int read(ByteBuffer[] columns)  throws IOException{
            
            if (!use_ssl)
                bytes_read = (int)sc.read(columns);
            else {
                bytes_read = 0;
                for (ByteBuffer column : columns) {
                    response_bytes = new byte[column.limit()];
                    bytes_read += in.read(response_bytes);
                    column.clear();
                    column.put(response_bytes);
                }
            }
            
            return bytes_read;
        }
        
        
        int write(ByteBuffer data)  throws IOException{
            
            if (use_ssl) {
                message_bytes = new byte[data.limit()];
                data.get(message_bytes);
                out.write(message_bytes);
            }
                
            return (use_ssl) ? message_bytes.length : sc.write(data);
        }
        
        
        void close() throws IOException {
            
            if (use_ssl) 
                ssl_socket.close(); 
            else 
                sc.close();
        }
    }
    //*/


    // Constructor  
    // -----------
    
    public Connector(String _ip, int _port, boolean _cluster, boolean _ssl) throws IOException, NoSuchAlgorithmException, KeyManagementException, ScriptException {
        /* JSON parsing engine setup, initial socket connection */
        
    	// https://stackoverflow.com/questions/25332640/getenginebynamenashorn-returns-null
        engine = new ScriptEngineManager(null).getEngineByName("javascript");
        json = (ScriptObjectMirror) engine.eval("JSON");
        engine_bindings = engine.getContext().getBindings(ScriptContext.GLOBAL_SCOPE);
        port = _port;
        ip = _ip;
        use_ssl = _ssl;
        
        s.connect(new InetSocketAddress(ip, port));
        //s.socket().setKeepAlive(true);
        
        // Clustered connection - reconnect to actual ip and port
        if (_cluster) {
            // Get data from server picker
            response_buffer.clear();
            bytes_read = s.read(response_buffer);
            response_buffer.flip();     
	    if (bytes_read == -1) {
		throw new IOException("Socket closed");
            }            
            // Read size of IP address (7-15 bytes) and get the IP
            byte [] ip_bytes = new byte[response_buffer.getInt()]; // Retreiving ip from clustered connection
            response_buffer.get(ip_bytes);
            ip = new String(ip_bytes, UTF8);

            // Last is the port
            port = response_buffer.getInt();
            
            // Close and reconnect socket to given ip and port
            s.close();
            
            s = SocketChannel.open();
            s.connect(new InetSocketAddress(ip, port));
        }
        // At this point we have a regular sokcet connected to the right address
        if (use_ssl) {
            ssl_context = SSLContext.getInstance("TLSv1.2");
            ssl_context.init(null,
               new TrustManager[]{ new X509TrustManager() {
                   public X509Certificate[] getAcceptedIssuers() {return null;}
                   public void checkClientTrusted(X509Certificate[] certs, String authType) 
                   {}
                   public void checkServerTrusted(X509Certificate[] certs, String authType)
                   {}
               }}, 
               null); 
            ss = ClientTlsChannel.newBuilder(s, ssl_context).build();
        }
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
    
    // (1) 
    //@SuppressWarnings("rawtypes")  // Remove "Map is a raw type" warning
    Map<String,Object> _parse_sqream_json(String json) throws ScriptException, ConnException { 
    	
    	String error;
    	
    	response_json = (Map<String, Object>) engine.eval(MessageFormat.format(json_wrapper, json));
        if (response_json.containsKey("error")) {
            error = (String)response_json.get("error");
            
            if (!error.contains("stop_statement could not find a statement"))
        		throw new ConnException("Error from SQream:" + error);
        }
    	
    	return response_json;
    }
    
    
    Boolean _validate_open(String statement_type) throws ConnException {
    	
    	if (!is_open()) { 
    		throw new ConnException("Trying to run command " + statement_type + " but connection closed");
    	}
    	
		if (!open_statement) { 
			throw new ConnException("Trying to run command " + statement_type + " but statement closed");
		}
    	
    	return true;
    }
    
    
    String _validate_response(String response, String expected) throws ConnException {
        
        if (!response.equals(expected))  // !response.contains("stop_statement could not find a statement")
            throw new ConnException("Expected message: " + expected + " but got " + response);
        
        return response;
    }
    
    // (2)  /* Return ByteBuffer with appropriate header for message */
    ByteBuffer _generate_headered_buffer(long data_length, boolean is_text_msg) {
        return ByteBuffer.allocate(10 + (int) data_length).order(ByteOrder.LITTLE_ENDIAN).put(protocol_version).put(is_text_msg ? (byte)1:(byte)2).putLong(data_length);
    }
    
    // (3)  /* Used by _send_data()  (merge if only one )  */
    int _get_parse_header() throws IOException, ConnException {
        header.clear();
        bytes_read = (use_ssl) ? ss.read(header) : s.read(header);
        // is connection open
        if (bytes_read == -1) {
  		throw new IOException("Socket closed");
 	}
        if (header.position() != 10)
        	throw new ConnException("bad header size from SQream - " + header.position() + ", possibly reading out of order");
        	
        header.flip();
        //print ("header: " + header);
    	protocol_version = header.get(); // java.nio.BufferUnderflowException - internal runtime error
    	if (protocol_version != 6)
        	throw new ConnException("bad protocol version returned - " + protocol_version + " perhaps an older version of SQream or reading out of oreder");
    	
    	is_text = header.get();
        response_length = header.getLong();
        
        return (int)response_length;
    }
    
    // (4) /* Manage actual sending and receiving of ByteBuffers over exising socket  */
    String _send_data (ByteBuffer data, boolean get_response) throws IOException, ConnException {
           /* Used by _send_message(), _flush()   */
        
        response_buffer.clear();    
        
        if (data != null ) {
            data.flip();
            while(data.hasRemaining()) 
                written = (use_ssl) ? ss.write(data) : s.write(data);
        }
        
        // Sending null for data will get us here directly, allowing to only get socket response if needed
        if(get_response) {
            response_message = ByteBuffer.allocate(_get_parse_header());
            bytes_read = (use_ssl) ? ss.read(response_message) : s.read(response_message);
            response_message.flip();
	    if (bytes_read == -1) {
                throw new IOException("Socket closed");
 	    }
        }   
        
        return (get_response) ? decode(response_message) : "" ;
    }
    
    // (5)   /* Send a JSON string to SQream over socket  */
    String _send_message(String message, boolean get_response) throws IOException, ConnException {
        
    	message_bytes = message.getBytes();
        message_buffer = _generate_headered_buffer((long)message_bytes.length, true);
        message_buffer.put(message_bytes);
        
        return _send_data(message_buffer, get_response);
    }
    
    
    // Internal API Functions
    // ----------------------------
    /*  
     * (1) _parse_query_type() - 
     * (2) _flush()
     */
    
    // ()  /* Unpack the json of column data arriving via queryType/(named). Called by prepare()  */
    //@SuppressWarnings("rawtypes") // "Map is a raw type" @ col_data = (Map)query_type.get(idx);
    void _parse_query_type(JSONListAdapter query_type) throws IOException, ScriptException{
        
        row_length = query_type.size();
        if(row_length ==0)
            return;
        
        // Set metadata arrays given the amount of columns
        col_names = new String[row_length];
        col_types = new String[row_length];
        col_sizes = new int[row_length];
        col_nullable = new BitSet(row_length);
        col_tvc = new BitSet(row_length);
        col_names_map = new HashMap<String, Integer>();
        
        col_calls = new int[row_length];
        // Parse the queryType json to get metadata for every column
        // An internal item looks like: {"isTrueVarChar":false,"nullable":true,"type":["ftInt",4,0]}
        for(int idx=0; idx < row_length; idx++) {
            // Parse JSON to correct objects
            col_data = (Map<String,Object>)query_type.get(idx);
            col_type_data = (JSONListAdapter)col_data.get("type"); // type is a list of 3 items
            
            // Assign data from parsed JSON objects to metadata arrays
            col_nullable.set(idx, (boolean)col_data.get("nullable")); 
            col_tvc.set(idx, (boolean)col_data.get("isTrueVarChar")); 
            col_names[idx] = statement_type.equals("SELECT") ? (String)col_data.get("name"): "denied";
            col_names_map.put(col_names[idx].toLowerCase(), idx +1);
            col_types[idx] = (String) col_type_data.get(0);
            col_sizes[idx] = (int) col_type_data.get(1);
        }
        
        // Create Storage for insert / select operations
        if (statement_type.equals("INSERT")) {
            // Calculate number of rows to flush at
            row_size = IntStream.of(col_sizes).sum() + col_nullable.cardinality();    // not calculating nvarc lengths for now
            rows_per_flush = FLUSH_SIZE / row_size;
            // rows_per_flush = 500000;
            // Buffer arrays for column storage
            data_columns = new ByteBuffer[row_length];
            null_columns = new ByteBuffer[row_length];
            null_resetter = ByteBuffer.allocate(rows_per_flush);
            nvarc_len_columns = new ByteBuffer[row_length];
            
            // Instantiate flags for managing network insert operations
            row_counter = 0;
            columns_set = new BitSet(row_length); // defaults to false
            
            // Initiate buffers for each column using the metadata
            for (int idx=0; idx < row_length; idx++) {
                data_columns[idx] = ByteBuffer.allocateDirect(col_sizes[idx]*rows_per_flush).order(ByteOrder.LITTLE_ENDIAN);
                null_columns[idx] = col_nullable.get(idx) ? ByteBuffer.allocateDirect(rows_per_flush).order(ByteOrder.LITTLE_ENDIAN) : null;
                nvarc_len_columns[idx] = col_tvc.get(idx) ? ByteBuffer.allocateDirect(4*rows_per_flush).order(ByteOrder.LITTLE_ENDIAN) : null;
            }
        }
        if (statement_type.equals("SELECT")) { 
            
            // Instantiate select counters, Initial storage same as insert
            row_counter = -1;
            total_rows_fetched = 0;
            data_columns = new ByteBuffer[row_length];
            //null_columns = new byte[row_length][];
            null_columns = new ByteBuffer[row_length];
            null_balls = new ByteBuffer[row_length];
            nvarc_len_columns = new ByteBuffer[row_length];     
            
            // Get the maximal string size (or size fo another type if strings are very small)
            string_bytes = new byte[Arrays.stream(col_sizes).max().getAsInt()];
        }
    }
    
    
    int _fetch() throws IOException, ScriptException, ConnException {
        /* Request and get data from SQream following a SELECT query */
        
        // Send fetch request and get metadata on data to be received
        response_json = _parse_sqream_json(_send_message(form_json("fetch"), true));
        new_rows_fetched = (int) response_json.get("rows");
        fetch_sizes =  (JSONListAdapter) response_json.get("colSzs");  // Chronological sizes of all rows recieved, only needed for nvarchars
        if (new_rows_fetched == 0)
            return new_rows_fetched;
        
        // Initiate storage columns using the "colSzs" returned by SQream
        // All buffers in a single array to use SocketChannel's read(ByteBuffer[] dsts)
        fetch_buffers = new ByteBuffer[fetch_sizes.size()]; 
        for (int idx=0; idx < fetch_sizes.size(); idx++) 
            fetch_buffers[idx] = ByteBuffer.allocateDirect((int)fetch_sizes.get(idx)).order(ByteOrder.LITTLE_ENDIAN);
            
        // Sort buffers to appropriate arrays (row_length determied during _query_type())
        for (int idx=0, buf_idx = 0; idx < row_length; idx++, buf_idx++) {  
            if(col_nullable.get(idx)) {
                null_columns[idx] = fetch_buffers[buf_idx];
                //fetch_buffers[buf_idx].get(null_columns[idx]);
                //fetch_buffers[buf_idx].clear();
                //null_balls[idx] = fetch_buffers[buf_idx];
                buf_idx++;
            } else
                null_columns[idx] = null;
            
            if(col_tvc.get(idx)) {
                nvarc_len_columns[idx] = fetch_buffers[buf_idx];
                buf_idx++;
            } else
                nvarc_len_columns[idx] = null;
            
            data_columns[idx] = fetch_buffers[buf_idx];
        }
        
        // Initial naive implememntation - Get all socket data in advance
        if (new_rows_fetched > 0) {  
            bytes_read = _get_parse_header();   // Get header out of the way
            do {
                bytes_read = (int)((use_ssl) ? ss.read(fetch_buffers) : s.read(fetch_buffers));
        	if (bytes_read == -1) {
                    throw new IOException("Socket closed");
 		}
                //print ("binary bytes read: " + bytes_read);
            }
            while (bytes_read >0);
            
            //Arrays.stream(fetch_buffers).forEach(fetched -> fetched.flip());
            for (ByteBuffer fetched : fetch_buffers) { 
                fetched.flip();
            }
        }
    
        
        return new_rows_fetched;  // counter nullified by next()
    }
    
    
    int _flush(int row_counter) throws IOException, ConnException {
        /* Send columnar data buffers to SQream. Called by next() and close() */
        
        if (!statement_type.equals("INSERT"))  // Not an insert statement
            return 0;
        
        // Send put message
        _send_message(MessageFormat.format(put, row_counter), false);   
        
        // Get total column length for the header
        total_bytes = 0;
        for(int idx=0; idx < row_length; idx++) {
            total_bytes += (null_columns[idx] != null) ? row_counter : 0;
            total_bytes += (nvarc_len_columns[idx] != null) ? 4 * row_counter : 0;

            total_bytes += data_columns[idx].position();
        }
        
        // Send header with total binary insert
        message_buffer = _generate_headered_buffer(total_bytes, false); 
        _send_data(message_buffer, false);
        
        // Send available columns
        for(int idx=0; idx < row_length; idx++) {
            if(null_columns[idx] != null) {
                _send_data((ByteBuffer)null_columns[idx].position(row_counter), false); 
            }
            if(nvarc_len_columns[idx] != null) {
                _send_data(nvarc_len_columns[idx], false);
            }
            _send_data(data_columns[idx], false);
        }
        
        _validate_response(_send_data(null, true), form_json("putted"));  // Get {"putted" : "putted"}
        
        
        return row_counter;  // counter nullified by next()
    }
    
    
    // User API Functions
    /* ------------------
     * connect(), execute(), next(), close(), close_conenction() 
     * 
     */
    
    public int connect(String _database, String _user, String _password, String _service) throws IOException, ScriptException, ConnException {
        //"'{'\"username\":\"{0}\", \"password\":\"{1}\", \"connectDatabase\":\"{2}\", \"service\":\"{3}\"'}'";
        
        database = _database;
        user = _user;
        password = _password;
        service = _service;
        
        String connStr = MessageFormat.format(connectDatabase, database, user, password, service);
        //response_json = _parse_sqream_json(_send_message(connStr, true));
        
        connection_id = (int) _parse_sqream_json(_send_message(connStr, true)).get("connectionId"); 

        return connection_id;
    }
    

    public int execute(String statement) throws IOException, ScriptException, ConnException {
        /* getStatementId, prepareStatement, reconnect, execute, queryType  */
        
    	if (open_statement)
    		throw new ConnException("Trying to run a statement when another was not closed");
    	open_statement = true;

        // Get statement ID, send prepareStatement and get response parameters
        statement_id = (int) _parse_sqream_json(_send_message(form_json("getStatementId"), true)).get("statementId");
        
        // Generating a valid json string via external library
        JsonObject prepare_jsonify;
        try
        {
	        prepare_jsonify = Json.object()
		    		.add("prepareStatement", statement)
		    		.add("chunkSize", 0);  // Random chunkSize to remember it's not really used
        }
    	catch(ParseException e)
        {
    		throw new ConnException ("Could not parse the statement for PrepareStatement");
        }
        
        // Jsonifying via standard library - verify with test
        //engine_bindings.put("statement", statement);
        //String prepareStr = (String) engine.eval("JSON.stringify({prepareStatement: statement, chunkSize: 0})");
        
        String prepareStr = prepare_jsonify.toString(WriterConfig.MINIMAL);
        
        response_json =  _parse_sqream_json(_send_message(prepareStr, true));
        
        // Parse response parameters
        listener_id =   (int) response_json.get("listener_id");
        port =          (int) response_json.get("port");
        port_ssl =      (int) response_json.get("port_ssl");
        reconnect =     (boolean) response_json.get("reconnect");
        ip =            (String) response_json.get("ip");
        
        port = use_ssl ? port_ssl : port; 
        // Reconnect and reestablish statement if redirected by load balancer
        if (reconnect) {
            // Closing and reconnecting socket to new ip / port
            if (use_ssl) 
                ss.close();
            s.close();
            
            s = SocketChannel.open();
            s.connect(new InetSocketAddress(ip, port));
            //s.socket().setKeepAlive(true);
            if (use_ssl)
                ss = ClientTlsChannel.newBuilder(s, ssl_context).build();
            
            // Sending reconnect, reconstruct commands
            String reconnectStr = MessageFormat.format(reconnectDatabase, database, user, password, service, connection_id, listener_id);
            _send_message(reconnectStr, true);      
            _validate_response(_send_message( MessageFormat.format(reconstructStatement, statement_id), true), form_json("statementReconstructed"));

        }  
         
        // Getting query type manouver and setting the type of query
        _validate_response(_send_message(form_json("execute"), true), form_json("executed"));  
        query_type =  (JSONListAdapter)_parse_sqream_json(_send_message(form_json("queryTypeIn"), true)).get("queryType");
        
        if (query_type.isEmpty()) {
            query_type =  (JSONListAdapter)_parse_sqream_json(_send_message(form_json("queryTypeOut"), true)).get("queryTypeNamed");
            statement_type = query_type.isEmpty() ? "DML" : "SELECT";
        }
        else {
            statement_type = "INSERT";
        }   
        
        // Select or Insert statement - parse queryType response for metadata
        if (!statement_type.equals("DML"))  
            _parse_query_type(query_type);
        
        
        return statement_id;
    }
    
    
    public boolean next() throws ConnException, IOException, ScriptException {
        /* See that all needed buffers were set, flush if needed, nullify relevant
           counters */
        
        if (statement_type.equals("INSERT")) {
                
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
                for(int idx=0; idx < row_length; idx++) {
                    if (null_columns[idx] != null) {  
                    	// Clear doesn't actually nullify/reset the data
                        null_columns[idx].clear();
                        null_columns[idx].put(null_resetter);
                        null_columns[idx].clear();
                    }
                    if(nvarc_len_columns[idx] != null) 
                        nvarc_len_columns[idx].clear();
                    data_columns[idx].clear();
                }
            }
        }
        else if (statement_type.equals("SELECT")) {
            //print ("select row counter: " + row_counter + " total: " + total_rows_fetched);
            // If all data has been read, try to fetch more
        	Arrays.fill(col_calls, 0); // calls in the same fetch - for varchar / nvarchar
        	if (row_counter == (total_rows_fetched -1)) {
                row_counter = -1;
                total_rows_fetched = _fetch();
                if (row_counter == (total_rows_fetched -1)) {
                    return false; // No more data and we've read all we have
                }
                
            }
            row_counter++;
        
        }
        else if (statement_type.equals("DML"))
            throw new ConnException ("Calling next() on a non insert / select query");
        
        else
            throw new ConnException ("Calling next() on a statement type different than INSERT / SELECT / DML: " + statement_type);
            
        return true;
    }
    
    
    public Boolean close() throws IOException, ScriptException, ConnException {
        
    	String res = "";
    	
    	if (is_open()) {
    		if (open_statement) {
    			
    			if (statement_type!= null && statement_type.equals("INSERT")) {
    	            _flush(row_counter);
    	        }
    	            // Statement is finished so no need to reset row_counter etc
    			
    			res = _validate_response(_send_message(form_json("closeStatement"), true), form_json("statementClosed"));
    	        open_statement = false;
    		}
    		else
    			return false;  //res =  "statement " + statement_id + " already closed";
    	}
    	else
    		return false;  //res =  "connection already closed";
        
        return true;
    }
    
    
    public boolean close_connection() throws IOException, ScriptException, ConnException {
        
    	// close();
        _validate_response(_send_message(form_json("closeConnection"), true), form_json("connectionClosed"));
        
        if (use_ssl) {
        	if (ss.isOpen())
        		ss.close(); // finish ssl communcication and close SSLEngine
        }
        
        if (s.isOpen())
        	s.close();
        
        return true;
    }
    
    boolean _validate_index(int col_num) throws ConnException {
    	if (col_num <0 || col_num > row_length)
    		 throw new ConnException("Illegal index on get/set\nAllowed indices are 0-" + (row_length -1));
    	
    	return true;
    }
    
    // Gets
    // ----
    
    boolean _validate_get(int col_num, String value_type) throws ConnException {
        /* If get function is appropriate, return true for non null values, false for a null */
        // Validate type
        if (!col_types[col_num].equals(value_type))
            throw new ConnException("Trying to get a value of type " + value_type + " from column number " + col_num + " of type " + col_types[col_num]);
        
        // print ("null column holder: " + Arrays.toString(null_columns));
        return null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0;
    }
    
    // -o-o-o-o-o    By index -o-o-o-o-o
    
    public Boolean get_boolean(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        return (_validate_get(col_num, "ftBool")) ? data_columns[col_num].get(row_counter) != 0 : null;
    }
    
    
    public Byte get_ubyte(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        return (_validate_get(col_num, "ftUByte")) ? data_columns[col_num].get(row_counter) : null;
    }  // .get().toUnsignedInt()  -->  to allow values between 127-255 
    
    
    public Short get_short(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	//*
    	if (col_types[col_num].equals("ftUByte"))
            return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (short)(data_columns[col_num].get(row_counter) & 0xFF) : null;
		//*/
		
    	return (_validate_get(col_num, "ftShort")) ? data_columns[col_num].getShort(row_counter * 2) : null;
    }
    
    
    public Integer get_int(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	//*
	    if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (int)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (int)(data_columns[col_num].get(row_counter) & 0xFF) : null;
		//*/
        return (_validate_get(col_num, "ftInt")) ? data_columns[col_num].getInt(row_counter * 4) : null;
        //return (null_balls[col_num].get() == 0) ? data_columns[col_num].getInt() : null;
    }
    
    
    public Long get_long(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	
    	if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (long)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
        return (_validate_get(col_num, "ftLong")) ? data_columns[col_num].getLong(row_counter * 8) : null;
    }
    
    
    public Float get_float(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        
    	if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (float)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
    	return (_validate_get(col_num, "ftFloat")) ? data_columns[col_num].getFloat(row_counter * 4) : null;
    }
    
    
    public Double get_double(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
    	
	    if (col_types[col_num].equals("ftFloat"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getFloat(row_counter * 4) : null;
        else if (col_types[col_num].equals("ftLong"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getLong(row_counter * 8) : null;
        else if (col_types[col_num].equals("ftInt"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getInt(row_counter * 4) : null;
    	else if (col_types[col_num].equals("ftShort"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)data_columns[col_num].getShort(row_counter * 2) : null;
	    else if (col_types[col_num].equals("ftUByte"))
	        return (null_columns[col_num] == null || null_columns[col_num].get(row_counter) == 0) ? (double)(data_columns[col_num].get(row_counter) & 0xFF) : null;
	        
        
        return (_validate_get(col_num, "ftDouble")) ? data_columns[col_num].getDouble(row_counter * 8) : null;
    }
    
    
    public String get_varchar(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Get bytes the size of the varchar column into string_bytes
        if (col_calls[col_num]++ > 0) {
	        // Resetting buffer position in csae someone runs the same get()
	        data_columns[col_num].position(data_columns[col_num].position() -col_sizes[col_num]);
        }
        data_columns[col_num].get(string_bytes, 0, col_sizes[col_num]);
        
        return (_validate_get(col_num, "ftVarchar")) ? new String(string_bytes, 0, col_sizes[col_num], UTF8).trim() : null;
    }
    
    
    public String get_nvarchar(int col_num) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        nvarc_len = nvarc_len_columns[col_num].getInt(row_counter * 4);
        
        // Get bytes the size of this specific nvarchar into string_bytes
        if (col_calls[col_num]++ > 0)
        	data_columns[col_num].position(data_columns[col_num].position() - nvarc_len);
        data_columns[col_num].get(string_bytes, 0, nvarc_len);
        
        return (_validate_get(col_num, "ftBlob")) ? new String(string_bytes, 0, nvarc_len, UTF8) : null;
    }
    
    
    public Date get_date(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
		_validate_index(col_num);
        
		return (_validate_get(col_num, "ftDate")) ? int_to_date(data_columns[col_num].getInt(4*row_counter), zone) : null;
    }
    
    
    public Timestamp get_datetime(int col_num, ZoneId zone) throws ConnException {   col_num--;  // set / get work with starting index 1
		_validate_index(col_num);
		return (_validate_get(col_num, "ftDateTime")) ? long_to_dt(data_columns[col_num].getLong(8* row_counter), zone) : null;
	}

    
    public Date get_date(int col_num) throws ConnException {  
    
    	return get_date(col_num, system_tz); // system_tz, UTC
    }
    
    
    public Timestamp get_datetime(int col_num) throws ConnException {   // set / get work with starting index 1
    	
        return get_datetime(col_num, system_tz); // system_tz, UTC
    }

    // -o-o-o-o-o  By column name -o-o-o-o-o
    
    public Boolean get_boolean(String col_name) throws ConnException {  
    	
        return get_boolean(col_names_map.get(col_name));
    }
    
    public Byte get_ubyte(String col_name) throws ConnException {  
    
        return get_ubyte(col_names_map.get(col_name));
    }
    
    public Short get_short(String col_name) throws ConnException {  
        
        return get_short(col_names_map.get(col_name));
    }
    
    public Integer get_int(String col_name) throws ConnException {     
        
        return get_int(col_names_map.get(col_name));
    }
    
    public Long get_long(String col_name) throws ConnException {  
    	
        return get_long(col_names_map.get(col_name));
    }
    
    public Float get_float(String col_name) throws ConnException {   
        
        return get_float(col_names_map.get(col_name));
    }
    
    public Double get_double(String col_name) throws ConnException {  
        
        return get_double(col_names_map.get(col_name));
    }
    
    public String get_varchar(String col_name) throws ConnException {  
        
        return get_varchar(col_names_map.get(col_name));
    }
    
    public String get_nvarchar(String col_name) throws ConnException {   
        
        return get_nvarchar(col_names_map.get(col_name));
    }
    
    public Date get_date(String col_name) throws ConnException {   
            
        return get_date(col_names_map.get(col_name));
    }
    
    public Timestamp get_datetime(String col_name) throws ConnException {   
        
        return get_datetime(col_names_map.get(col_name));
    }
    
    // Sets
    // ----
    
    boolean _validate_set(int col_num, Object value, String value_type) throws ConnException {
        
        // Validate type
        if (!col_types[col_num].equals(value_type))
            throw new ConnException("Trying to set " + value_type + " on a column number " + col_num + " of type " + col_types[col_num]);
    
        // Optional null handling - if null is appropriate, mark null column
        if (value == null) {
            if (null_columns[col_num] != null) { 
                null_columns[col_num].put((byte)1);
                is_null = true;
            } else
                throw new ConnException("Trying to set null on a non nullable column of type " + col_types[col_num]);
        }
        else
            is_null = false;
        
        
        return is_null;
    }


    public boolean set_boolean(int col_num, Boolean value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].put((byte)(_validate_set(col_num, value, "ftBool") ? 0 : (value == true) ? 1 : 0));
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_ubyte(int col_num, Byte value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Check the byte is positive
        if (value!= null && value < 0 ) 
                throw new ConnException("Trying to set a negative byte value on an unsigned byte column");
        
        // Set actual value - null or positive at this point
        data_columns[col_num].put(_validate_set(col_num, value, "ftUByte") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
     
    public boolean set_short(int col_num, Short value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putShort(_validate_set(col_num, value, "ftShort") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
        
    
    public boolean set_int(int col_num, Integer value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putInt(_validate_set(col_num, value, "ftInt") ? 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
     

    public boolean set_long(int col_num, Long value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putLong(_validate_set(col_num, value, "ftLong") ? (long) 0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
     
    
    public boolean set_float(int col_num, Float value) throws ConnException {   col_num--;  // set / get work with starting index 1
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putFloat(_validate_set(col_num, value, "ftFloat") ? (float)0.0 : value);
        
        // Mark column as set (BitSet at location col_num set to true
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_double(int col_num, Double value) throws ConnException {  col_num--;
    	_validate_index(col_num);
        // Set actual value
        data_columns[col_num].putDouble(_validate_set(col_num, value, "ftDouble") ? 0.0 : value);
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_varchar(int col_num, String value) throws ConnException {  col_num--;
    	_validate_index(col_num);
        // Set actual value - padding with spaces to the left if needed
        string_bytes = _validate_set(col_num, value, "ftVarchar") ? "".getBytes(UTF8) : value.getBytes(UTF8);
        if (string_bytes.length > col_sizes[col_num]) 
            throw new ConnException("Trying to set string of size " + string_bytes.length + " on column of size " +  col_sizes[col_num] );
        // Generate missing spaces to fill up to size
        spaces = new byte[col_sizes[col_num] - string_bytes.length];
        Arrays.fill(spaces, (byte) 32);  // ascii value of space
        
        // Set value and added spaces if needed
        data_columns[col_num].put(string_bytes);
        data_columns[col_num].put(spaces);
        // data_columns[col_num].put(String.format("%-" + col_sizes[col_num] + "s", value).getBytes());
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_nvarchar(int col_num, String value) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        // Convert string to bytes
        string_bytes = _validate_set(col_num, value, "ftBlob") ? "".getBytes(UTF8) : value.getBytes(UTF8);
                
        // Add string length to lengths column
        nvarc_len_columns[col_num].putInt(string_bytes.length);
        
        // Set actual value
        data_columns[col_num].put(string_bytes);
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_date(int col_num, Date date, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        
    	// Set actual value
        data_columns[col_num].putInt(_validate_set(col_num, date, "ftDate") ? 0 : date_to_int(date, zone));
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
        
    
    public boolean set_datetime(int col_num, Timestamp ts, ZoneId zone) throws ConnException, UnsupportedEncodingException {  col_num--;
    	_validate_index(col_num);
        
    	//ZonedDateTime dt = ts.toLocalDateTime().atZone(zone); 
    	// ZonedDateTime dt = ts.toInstant().atZone(zone); 

    	// Set actual value
        data_columns[col_num].putLong(_validate_set(col_num, ts, "ftDateTime") ? 0 : dt_to_long(ts, zone));
        
        // Mark column as set
        columns_set.set(col_num);
        
        return true;
    }
    
    
    public boolean set_date(int col_num, Date value) throws ConnException, UnsupportedEncodingException { 
        
        return set_date(col_num, value, system_tz); // system_tz, UTC
    }
        
    
    public boolean set_datetime(int col_num, Timestamp value) throws ConnException, UnsupportedEncodingException {  
    	
        return set_datetime(col_num, value, system_tz); // system_tz, UTC
}
    
    // Metadata
    // --------
    
    int _validate_col_num(int col_num) throws ConnException {
        
        if (col_num <1) 
            throw new ConnException ("Using a metadata function with a non positive column value");
        
        return --col_num;
    }
    
    public int get_statement_id() {

        return statement_id;
    }


    public String get_query_type() {
        
        return statement_type;
    }
    
    public int get_row_length() {  // number of columns for this query
        
        return row_length;
    }
    
    public String get_col_name(int col_num) throws ConnException {  
    
        return col_names[_validate_col_num(col_num)];
    }
        
    public String get_col_type(int col_num) throws ConnException {  
        
    	
    	
        return col_types[_validate_col_num(col_num)];
    }
    
    
    public String get_col_type(String col_name) throws ConnException {  
        
    	Integer col_num = col_names_map.get(col_name);
    	if (col_num == null)
    		throw new ConnException("\nno column found for name: " + col_name + "\nExisting columns: \n" + col_names_map.keySet());
    	
        return get_col_type(col_names_map.get(col_name));
    }

    
    public int get_col_size(int col_num) throws ConnException {  
    
        return col_sizes[_validate_col_num(col_num)];
    }
        
    public boolean is_col_nullable(int col_num) throws ConnException { 
        
        return col_nullable.get(_validate_col_num(col_num));
    }
    
    public boolean is_open_statement() {
        
        return open_statement;
    }
    
    public boolean is_open() {
        
        return (use_ssl) ? ss.isOpen() : s.isOpen();
    }
    
    // Main
    // ----
    
    public static void main(String[] args) throws ScriptException, IOException, ConnException, NoSuchAlgorithmException, KeyManagementException {
        
    }

}
