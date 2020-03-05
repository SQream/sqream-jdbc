package com.sqream.jdbc.connector;

import com.sqream.jdbc.connector.socket.SQSocketConnector;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;


@RunWith(PowerMockRunner.class)
@PrepareForTest({SQSocketConnector.class})
public class SQSocketConnectorTest {

                                                //TODO: Alex K 03/05/2020 rewrite this test
    @Test
    public void reconnectToNodeTest() throws NoSuchAlgorithmException, IOException, KeyManagementException {
//        boolean USE_SSL = false;
//        boolean CLUSTER = true;
//        PowerMockito.mockStatic(SQSocketConnector.class);
//        SQSocketConnector connectorMock = Mockito.mock(SQSocketConnector.class);
//        Mockito.when(connectorMock.read(any(ByteBuffer.class))).thenAnswer(new Answer<Void>() {
//            @Override
//            public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
//                ByteBuffer targetBuffer = invocationOnMock.getArgument(0);
//                byte[] ip = IP.getBytes(StandardCharsets.UTF_8);
//                targetBuffer.putInt(ip.length);
//                targetBuffer.put(ip);
//                targetBuffer.putInt(PORT);
//                return null;
//            }
//        });
//
//        PowerMockito.when(SQSocketConnector.connect(IP, PORT, USE_SSL)).thenReturn(connectorMock);
//
//        new ConnectorImpl(IP, PORT, CLUSTER, USE_SSL);
//
//        Mockito.verify(connectorMock).reconnect(IP, PORT, USE_SSL);
    }


                                                //TODO: Alex K 03/05/2020 rewrite this test
//    @Test(expected = IOException.class)
    public void whenReconnectToNodeButDidNotReadBytesFromSocketThenThrowExceptionTest()
            throws NoSuchAlgorithmException, IOException, KeyManagementException {

//        String CORRECT_MESSAGE = "Socket closed When trying to connect to server picker";
//
//        try {
//
//            boolean USE_SSL = false;
//            boolean CLUSTER = true;
//            PowerMockito.mockStatic(SQSocketConnector.class);
//            SQSocketConnector connectorMock = Mockito.mock(SQSocketConnector.class);
//            Mockito.when(connectorMock.read(any(ByteBuffer.class))).thenReturn(-1);
//
//            PowerMockito.when(SQSocketConnector.connect(IP, PORT, USE_SSL)).thenReturn(connectorMock);
//
//            new ConnectorImpl(IP, PORT, CLUSTER, USE_SSL);
//        } catch (IOException e) {
//            if (CORRECT_MESSAGE.equals(e.getMessage())) {
//                throw e;
//            } else {
//                fail(MessageFormat.format("Method throws incorrect exception message [{0}}. Should be [{1}]", e.getMessage(), CORRECT_MESSAGE));
//            }
//        }
//        fail("Method should throw exception");
    }
}