package com.sqream.jdbc.connector;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import com.sqream.jdbc.connector.socket.SQSocket;
import com.sqream.jdbc.connector.socket.SQSocketConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.Arrays;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.connector.socket.SQSocketConnector.SUPPORTED_PROTOCOLS;
import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SQSocketConnector.class, SQSocket.class, SSLContext.class, MessengerImpl.class})
public class SQSocketConnectorTest {

    @Test
    public void reconnectToNodeTest() throws ConnException {
        boolean USE_SSL = false;
        boolean CLUSTER = true;
        MessengerImpl messengerMock = Mockito.mock(MessengerImpl.class);
        Mockito.when(messengerMock.connect(any(), any(), any(), any())).thenReturn(new ConnectionStateDto(1, "123"));
        SQSocket socketMock = Mockito.mock(SQSocket.class);
        Mockito.when(socketMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
                byte[] ip = IP.getBytes(StandardCharsets.UTF_8);
                targetBuffer.putInt(ip.length);
                targetBuffer.put(ip);
                targetBuffer.putInt(PORT);
                return null;
            }
        });
        PowerMockito.mockStatic(SQSocket.class);
        PowerMockito.when(SQSocket.connect(IP, PORT, USE_SSL)).thenReturn(socketMock);
        PowerMockito.mockStatic(MessengerImpl.class);
        PowerMockito.when(MessengerImpl.getInstance(any())).thenReturn(messengerMock);

        Connector connector = new ConnectorImpl(
                ConnectionParams.builder()
                        .ipAddress(IP)
                        .port(String.valueOf(PORT))
                        .cluster(String.valueOf(CLUSTER))
                        .useSsl(String.valueOf(SSL))
                        .build());
        connector.connect(IP, USER, PASS, SERVICE);

        Mockito.verify(socketMock).close();
    }

    @Test(expected = ConnException.class)
    public void whenReconnectToNodeButDidNotReadBytesFromSocketThenThrowExceptionTest() throws ConnException {

        String CORRECT_MESSAGE = "Socket closed When trying to connect to server picker";

        try {

            boolean USE_SSL = false;
            boolean CLUSTER = true;
            SQSocket socketMock = Mockito.mock(SQSocket.class);
            Mockito.when(socketMock.read(any(ByteBuffer.class))).thenReturn(-1);
            PowerMockito.mockStatic(SQSocket.class);
            PowerMockito.when(SQSocket.connect(IP, PORT, USE_SSL)).thenReturn(socketMock);

            Connector connector = new ConnectorImpl(
                    ConnectionParams.builder()
                            .ipAddress(IP)
                            .port(String.valueOf(PORT))
                            .cluster(String.valueOf(CLUSTER))
                            .useSsl(String.valueOf(SSL))
                            .build());
            connector.connect(DATABASE, USER, PASS, SERVICE);

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

    //TODO: When we connect to server picker BUT did not provide param 'cluster=true', then read wrong data from socket.
    // Server sent ip address and port to reconnect, but client read first byte as protocol version (actually size of ip address)
    @Test(expected = ConnException.class)
    public void whenConnectToClusterWithParamClusterFalseTest()
            throws IOException, ConnException, KeyManagementException, NoSuchAlgorithmException {

        SQSocket socketMock = Mockito.mock(SQSocket.class);
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

        PowerMockito.mockStatic(SQSocket.class);
        PowerMockito.when(SQSocket.connect(IP, PORT, false)).thenReturn(socketMock);
        PowerMockito.mockStatic(SSLContext.class);
        PowerMockito.when(SSLContext.getDefault()).thenReturn(null);
        SQSocketConnector socketConnector = SQSocketConnector.connect(IP, PORT, false, false);

        try {
            socketConnector.parseHeader();
        } catch (ConnException e) {
            assertTrue(e.getMessage().contains("Probably tried to connect to server picker"));
            throw e;
        }
    }

    @Test
    public void supportedProtocolsTest() throws IOException, ConnException, NoSuchAlgorithmException {

        for (byte protocolVersion : SUPPORTED_PROTOCOLS) {
            SQSocket socketMock = Mockito.mock(SQSocket.class);
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

            PowerMockito.mockStatic(SQSocket.class);
            PowerMockito.when(SQSocket.connect(IP, PORT, false)).thenReturn(socketMock);
            PowerMockito.mockStatic(SSLContext.class);
            PowerMockito.when(SSLContext.getDefault()).thenReturn(null);
            SQSocketConnector socketConnector = SQSocketConnector.connect(IP, PORT, false, false);

            socketConnector.parseHeader();
        }
    }

    @Test(expected = ConnException.class)
    public void unsupportedProtocolTest() throws IOException, ConnException, NoSuchAlgorithmException {
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

        PowerMockito.mockStatic(SQSocket.class);
        PowerMockito.when(SQSocket.connect(IP, PORT, false)).thenReturn(socketMock);
        PowerMockito.mockStatic(SSLContext.class);
        PowerMockito.when(SSLContext.getDefault()).thenReturn(null);
        SQSocketConnector socketConnector = SQSocketConnector.connect(IP, PORT, false, false);

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
