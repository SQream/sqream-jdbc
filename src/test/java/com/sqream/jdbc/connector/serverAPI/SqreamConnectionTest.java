package com.sqream.jdbc.connector.serverAPI;

import com.sqream.jdbc.ConnectionParams;
import com.sqream.jdbc.connector.messenger.Messenger;
import com.sqream.jdbc.connector.messenger.MessengerImpl;
import org.junit.Test;
import org.mockito.Mockito;

import static com.sqream.jdbc.TestEnvironment.*;
import static com.sqream.jdbc.TestEnvironment.SSL;

public class SqreamConnectionTest {

    @Test
    public void whenCallCloseThenCloseConnectionTest() throws Exception {
        Messenger messengerMock = Mockito.mock(MessengerImpl.class);
        SqreamConnectionContext context = new SqreamConnectionContext(null, null, 1, messengerMock);
        SqreamConnection connection = new SqreamConnection(context);

        connection.close();

        Mockito.verify(messengerMock, Mockito.times(1)).closeConnection();
    }

    @Test
    public void whenCallCloseTwiceThenSecondIgnoredTest() throws Exception {
        Messenger messengerMock = Mockito.mock(MessengerImpl.class);
        SqreamConnectionContext context = new SqreamConnectionContext(null, null, 1, messengerMock);
        SqreamConnection connection = new SqreamConnection(context);

        connection.close();
        connection.close();

        Mockito.verify(messengerMock, Mockito.times(1)).closeConnection();
    }

    @Test(expected = RuntimeException.class)
    public void whenCallCreateStatementTwiceThenSecondThrowsExceptionTest() throws Exception {
        ConnectionParams connParams = ConnectionParams.builder()
                .ipAddress(IP)
                .port(String.valueOf(PORT))
                .cluster(String.valueOf(CLUSTER))
                .useSsl(String.valueOf(SSL))
                .dbName(DATABASE)
                .user(USER)
                .password(PASS)
                .service(SERVICE)
                .build();

        try (SqreamConnection conn = SqreamConnectionFactory.openConnection(connParams)) {

            conn.createStatement();
            conn.createStatement();
        }
    }
}
