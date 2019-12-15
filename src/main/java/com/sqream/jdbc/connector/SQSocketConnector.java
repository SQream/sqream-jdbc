package com.sqream.jdbc.connector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

import static com.sqream.jdbc.utils.Utils.decode;

class SQSocketConnector extends SQSocket {

    private static final byte PROTOCOL_VERSION = 7;
    private static final int HEADER_SIZE = 10;
    private static final List<Byte> SUPPORTED_PROTOCOLS = new ArrayList<>(Arrays.asList((byte)6, (byte)7));

    private ByteBuffer responseMessage = ByteBuffer.allocateDirect(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);;
    private ByteBuffer header = ByteBuffer.allocateDirect(10).order(ByteOrder.LITTLE_ENDIAN);

    SQSocketConnector(String ip, int port) throws IOException, NoSuchAlgorithmException {
        super(ip, port);
    }

    // (2)  /* Return ByteBuffer with appropriate header for message */
    ByteBuffer generateHeaderedBuffer(long dataLength, boolean is_text_msg) {

        return ByteBuffer.allocate(10 + (int) dataLength).order(ByteOrder.LITTLE_ENDIAN).put(PROTOCOL_VERSION).put(is_text_msg ? (byte)1:(byte)2).putLong(dataLength);
    }

    // (3)  /* Used by _send_data()  (merge if only one )  */
    int getParseHeader() throws IOException, ConnException {

        this.header.clear();
        readData(header, HEADER_SIZE);

        //print ("header: " + header);
        byte userProtocolVersion = header.get();
        if (!SUPPORTED_PROTOCOLS.contains(userProtocolVersion)) {
            StringJoiner joiner = new StringJoiner(", ");
            SUPPORTED_PROTOCOLS.forEach(newElement -> joiner.add(newElement.toString()));
            throw new ConnException(String.format("Unsupported protocol version - supported versions are %s, but got %s", joiner.toString(), userProtocolVersion));
        }

        header.get();  // Catching the 2nd byte of a response
        long responseLength = this.header.getLong();

        return (int) responseLength;
    }

    // (4) /* Manage actual sending and receiving of ByteBuffers over exising socket  */
    String sendData(ByteBuffer data, boolean get_response) throws IOException, ConnException {

        if (data != null ) {
            data.flip();
            while(data.hasRemaining()) {
                super.write(data);
            }
        }

        // Sending null for data will get us here directly, allowing to only get socket response if needed
        if(get_response) {
            int msg_len = getParseHeader();
            if (msg_len > 64000) // If our 64K response_message buffer doesn't do
            responseMessage = ByteBuffer.allocate(msg_len);
            responseMessage.clear();
            responseMessage.limit(msg_len);
            readData(responseMessage, msg_len);
        }

        return (get_response) ? decode(responseMessage) : "" ;
    }

    // (5)   /* Send a JSON string to SQream over socket  */
    String sendMessage(String message, boolean getResponse) throws IOException, ConnException {

        byte[] messageBytes = message.getBytes();
        ByteBuffer messageBuffer = generateHeaderedBuffer(messageBytes.length, true);
        messageBuffer.put(messageBytes);

        return sendData(messageBuffer, getResponse);
    }

    int readData(ByteBuffer response, int msgLen) throws IOException, ConnException {
        /* Read either a specific amount of data, or until socket is empty if msg_len is 0.
         * response ByteBuffer of a fitting size should be supplied.
         */
        if (msgLen > response.capacity()) {
            throw new ConnException("Attempting to read more data than supplied bytebuffer allows");
        }

        int totalBytesRead = 0;

        while (totalBytesRead < msgLen || msgLen == 0) {
            int bytesRead = super.read(response);
            if (bytesRead == -1) {
                throw new IOException("Socket closed. Last buffer written: " + response);
            }
            totalBytesRead += bytesRead;

            if (msgLen == 0 && bytesRead == 0) {
                break;  // Drain mode, read all that was available
            }
        }

        response.flip();  // reset position to allow reading from buffer

        return totalBytesRead;
    }
}
