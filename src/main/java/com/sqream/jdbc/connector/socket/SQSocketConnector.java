package com.sqream.jdbc.connector.socket;

import com.sqream.jdbc.connector.ConnException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static com.sqream.jdbc.utils.Utils.decode;

public class SQSocketConnector {

    private static final byte PROTOCOL_VERSION = 8;
    private static final int HEADER_SIZE = 10;
    public static final List<Byte> SUPPORTED_PROTOCOLS = new ArrayList<>(Arrays.asList((byte)6, (byte)7, (byte)8));

    private ByteBuffer responseMessage = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);;
    private ByteBuffer header = ByteBuffer.allocateDirect(10).order(ByteOrder.LITTLE_ENDIAN);

    private SQSocket socket;

    private SQSocketConnector(String ip, int port, boolean useSsl, boolean cluster) throws ConnException {
        socket = SQSocket.connect(ip, port, useSsl);
        // Clustered connection - reconnect to actual ip and port
        if (cluster) {
            reconnectToNode(useSsl);
        }
    }

    public static SQSocketConnector connect(String ip, int port, boolean useSsl, boolean cluster) throws ConnException {
        return new SQSocketConnector(ip, port, useSsl, cluster);
    }

    public void reconnect(String ip, int port, boolean useSsl) throws ConnException {
        socket.close();
        socket = SQSocket.connect(ip, port, useSsl);
    }

    // (2)  /* Return ByteBuffer with appropriate header for message */

    public ByteBuffer generateHeaderedBuffer(long dataLength, boolean is_text_msg) {

        return ByteBuffer.allocate(10 + (int) dataLength).order(ByteOrder.LITTLE_ENDIAN).put(PROTOCOL_VERSION).put(is_text_msg ? (byte)1:(byte)2).putLong(dataLength);
    }

    public ByteBuffer generateHeader(long dataLength, boolean is_text_msg) {

        return ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN).put(PROTOCOL_VERSION).put(is_text_msg ? (byte)1:(byte)2).putLong(dataLength);
    }
    // (3)  /* Used by _send_data()  (merge if only one )  */

    public int parseHeader() throws ConnException {

        this.header.clear();
        readData(header, HEADER_SIZE);

        //print ("header: " + header);
        byte userProtocolVersion = header.get();
        if (!SUPPORTED_PROTOCOLS.contains(userProtocolVersion)) {
            StringJoiner joiner = new StringJoiner(", ");
            SUPPORTED_PROTOCOLS.forEach(newElement -> joiner.add(newElement.toString()));
            if (isRedirection(header)) {
                throw new ConnException("Probably tried to connect to server picker, but cluster parameter was not provided");
            } else {
                throw new ConnException(String.format("Unsupported protocol version - supported versions are %s, but got %s", joiner.toString(), userProtocolVersion));
            }
        }

        header.get();  // Catching the 2nd byte of a response
        long responseLength = this.header.getLong();

        return (int) responseLength;
    }
    // (4) /* Manage actual sending and receiving of ByteBuffers over exising socket  */

    public String sendData(ByteBuffer data, boolean get_response) throws ConnException {

        if (data != null ) {
            data.flip();
            while(data.hasRemaining()) {
                socket.write(data);
            }
        }

        // Sending null for data will get us here directly, allowing to only get socket response if needed
        if(get_response) {
            int msg_len = parseHeader();
            if (msg_len > 64000) // If our 64K response_message buffer doesn't do
            responseMessage = ByteBuffer.allocate(msg_len);
            responseMessage.clear();
            responseMessage.limit(msg_len);
            readData(responseMessage, msg_len);
        }

        return (get_response) ? decode(responseMessage) : "" ;
    }
    // (5)   /* Send a JSON string to SQream over socket  */
    public int readData(ByteBuffer response, int msgLen) throws ConnException {
        /* Read either a specific amount of data, or until socket is empty if msg_len is 0.
         * response ByteBuffer of a fitting size should be supplied.
         */
        if (msgLen > response.capacity()) {
            throw new ConnException("Attempting to read more data than supplied bytebuffer allows");
        }

        int totalBytesRead = 0;

        while (totalBytesRead < msgLen || msgLen == 0) {
            int bytesRead = socket.read(response);
            if (bytesRead == -1) {
                throw new ConnException("Socket closed. Last buffer written: " + response);
            }
            totalBytesRead += bytesRead;

            if (msgLen == 0 && bytesRead == 0) {
                break;  // Drain mode, read all that was available
            }
        }

        response.flip();  // reset position to allow reading from buffer

        return totalBytesRead;
    }

    public void close() throws ConnException {
        socket.close();
    }

    public boolean isOpen() {
        return socket.isOpen();
    }

    private void reconnectToNode(boolean useSsl) throws ConnException {
        ByteBuffer response_buffer = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
        // Get data from server picker
        response_buffer.clear();
        //_read_data(response_buffer, 0); // IP address size may vary
        int bytes_read = socket.read(response_buffer);
        response_buffer.flip();
        if (bytes_read == -1) {
            throw new ConnException("Socket closed When trying to connect to server picker");
        }

        // Read size of IP address (7-15 bytes) and get the IP
        byte [] ip_bytes = new byte[response_buffer.getInt()]; // Retreiving ip from clustered connection
        response_buffer.get(ip_bytes);
        String ip = new String(ip_bytes, StandardCharsets.UTF_8);

        // Last is the port
        int port = response_buffer.getInt();

        socket.close();
        socket = SQSocket.connect(ip, port, useSsl);
    }

    // In case when try to connect to server picket, but cluster parameter was not provided.
    // First int is node ip address, but try to read first byte as protocol version.
    private boolean isRedirection(ByteBuffer header) {
        header.position(0);
        int firstByteAsInt = (int) header.get();
        header.position(0);
        int firstInt = header.getInt();
        return firstByteAsInt == firstInt;
    }
}
