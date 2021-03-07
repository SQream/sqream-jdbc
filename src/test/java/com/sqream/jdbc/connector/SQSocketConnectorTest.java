package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.socket.SQSocket;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Arrays;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.connector.socket.SQSocketConnector.SUPPORTED_PROTOCOLS;
import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class SQSocketConnectorTest {
    @Mock
    private SQSocket socketMock;


    @Test
    public void reconnectToNodeTest() throws ConnException {
        String IP_TO_CONNECT = "1.1.1.1";
        String IP_TO_RECONNECT = "2.2.2.2";
        int PORT_TO_CONNECT = 1234;
        int PORT_TO_RECONNECT = 4321;
        boolean USE_SSL = true;

        Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                byte[] ip = IP_TO_RECONNECT.getBytes(StandardCharsets.UTF_8);
                targetBuffer.putInt(ip.length);
                targetBuffer.put(ip);
                targetBuffer.putInt(PORT_TO_RECONNECT);
                return null;
            }
        });
        SQSocketConnector connector = new SQSocketConnector(socketMock);
        connector.connect(IP_TO_CONNECT, PORT_TO_CONNECT, USE_SSL, true);
        Mockito.verify(socketMock).open(IP_TO_CONNECT, PORT_TO_CONNECT, USE_SSL); // connect to server picker
        Mockito.verify(socketMock).close(); // close first connection to server picker
        Mockito.verify(socketMock).open(IP_TO_RECONNECT, PORT_TO_RECONNECT, USE_SSL); // reconnect to node
    }

    @Test(expected = ConnException.class)
    public void whenReconnectToNodeButDidNotReadBytesFromSocketThenThrowExceptionTest() throws ConnException {

        String CORRECT_MESSAGE =
                "Connection timed out. For cluster=true it is recommended to use port 3108 or 3109 for ssl.";
        int BYTES_HAS_READ = -1; // reached end of stream
        Mockito.when(socketMock.read(any())).thenReturn(BYTES_HAS_READ);

        try {
            SQSocketConnector socketConnector = new SQSocketConnector(socketMock);
            socketConnector.connect(null, 0, false, true);
        } catch (ConnException e) {
            if (CORRECT_MESSAGE.equals(e.getMessage())) {
                throw e;
            } else {
                fail(MessageFormat.format("Method throws incorrect exception message [{0}}. Should be [{1}]",
                        e.getMessage(), CORRECT_MESSAGE));
            }
        }
        fail("Method should throw exception");
    }

    /**
     * When we connect to server picker BUT did not provide param 'cluster=true', then read wrong data from socket.
     * Server sent ip address and port to reconnect, but client read first byte as protocol version (actually size of ip address)
     */
    @Test(expected = ConnException.class)
    public void whenConnectToClusterWithParamClusterFalseTest()
            throws ConnException, NoSuchAlgorithmException {

        Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                byte[] ip = IP.getBytes(StandardCharsets.UTF_8);
                targetBuffer.putInt(ip.length);
                targetBuffer.put(Arrays.copyOfRange(ip, 0, targetBuffer.capacity() - targetBuffer.position()));
                return targetBuffer.capacity();
            }
        });

        SQSocketConnector socketConnector = new SQSocketConnector(socketMock);
        socketConnector.connect(IP, PORT, false, false);

        try {
            socketConnector.parseHeader();
        } catch (ConnException e) {
            assertTrue(e.getMessage().contains("Connection error. Connected to cluster but cluster=false is in connection string, did you mean cluster=true?"));
            throw e;
        }
    }

    @Test
    public void supportedProtocolsTest() throws ConnException {

        for (byte protocolVersion : SUPPORTED_PROTOCOLS) {
            Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
                @Override
                public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                    ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                    targetBuffer.put(protocolVersion);
                    targetBuffer.put((byte) 0);
                    targetBuffer.putLong(42L);
                    return targetBuffer.capacity();
                }
            });

            SQSocketConnector socketConnector = new SQSocketConnector(socketMock);
            socketConnector.connect(IP, PORT, false, false);

            socketConnector.parseHeader();
        }
    }

    @Test(expected = ConnException.class)
    public void unsupportedProtocolTest() throws ConnException, NoSuchAlgorithmException {
        byte UNSUPPORTED_PROTOCOL_VERSION = (byte) 1;

        SQSocket socketMock = Mockito.mock(SQSocket.class);
        Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
            @Override
            public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                targetBuffer.put(UNSUPPORTED_PROTOCOL_VERSION);
                targetBuffer.put((byte) 0);
                targetBuffer.putLong(42L);
                return targetBuffer.capacity();
            }
        });

        SQSocketConnector socketConnector = new SQSocketConnector(socketMock);
        socketConnector.connect(IP, PORT, false, false);

        try {
            socketConnector.parseHeader();
        } catch (ConnException e) {
            if (e.getMessage().contains("Unsupported protocol version")) {
                throw e;
            } else {
                fail(MessageFormat.format("Incorrect exception message [{0}]", e.getMessage()));
            }
        }
    }
}
