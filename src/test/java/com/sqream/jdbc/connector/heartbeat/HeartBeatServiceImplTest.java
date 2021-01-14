package com.sqream.jdbc.connector.heartbeat;

import com.sqream.jdbc.connector.ConnException;
import com.sqream.jdbc.connector.messenger.Messenger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HeartBeatServiceImplTest {
    @Mock
    private Messenger messenger;

    @Test
    public void sendPingWithIntervalTest() throws Exception {
        long intervalMs = 10;
        long testRunsMs = 100;
        int expectedPingTimesAtLest = 7;
        int expectedPingTimesAtMost = 10;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.stop();

        Mockito.verify(messenger, Mockito.atLeast(expectedPingTimesAtLest)).ping();
        Mockito.verify(messenger, Mockito.atMost(expectedPingTimesAtMost)).ping();
    }

    @Test
    public void closeServiceTest() throws Exception {
        long intervalMs = 10;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        heartBeatService.start();
        Thread.sleep(intervalMs * 3);
        Mockito.verify(messenger, Mockito.atLeastOnce()).ping(); // verify that service started to ping
        heartBeatService.stop();
        Mockito.verify(messenger, Mockito.atLeastOnce()).ping(); // verify that service stopped to ping
    }

    @Test
    public void closeServiceTwiceTest() throws Exception {
        long intervalMs = 10;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        heartBeatService.start();
        Thread.sleep(intervalMs * 3);
        heartBeatService.stop();
        heartBeatService.stop();
    }

    @Test
    public void whenStartAlreadyRunningServiceThanKeepRunningTest() throws Exception {
        long intervalMs = 10;
        long testRunsMs = 100;
        int expectedPingTimesAtLest = 7 * 2;
        int expectedPingTimesAtMost = 10 * 2;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        // started service twice
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.stop();

        //even if started twice, should send once per interval
        Mockito.verify(messenger, Mockito.atLeast(expectedPingTimesAtLest)).ping();
        Mockito.verify(messenger, Mockito.atMost(expectedPingTimesAtMost)).ping();
    }

    @Test
    public void reuseServiceTest() throws Exception {
        long intervalMs = 10;
        long testRunsMs = 100;
        int expectedPingTimesAtLest = 7 * 2;
        int expectedPingTimesAtMost = 10 * 2;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        // start/stop service few times
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.stop();
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.stop();

        //even if started twice, should send once per interval
        Mockito.verify(messenger, Mockito.atLeast(expectedPingTimesAtLest)).ping();
        Mockito.verify(messenger, Mockito.atMost(expectedPingTimesAtMost)).ping();
    }

    @Test
    public void whenExecutionLessThenIntervalThenNothingSentTest() throws Exception {
        long intervalMs = 100;
        long testRunsMs = 50;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        heartBeatService.stop();

        Mockito.verify(messenger, Mockito.never()).ping();
    }

    @Test
    public void whenGetExceptionThenStopServiceSilentlyTest() throws ConnException, InterruptedException {
        long intervalMs = 10;
        long testRunsMs = 100;

        HeartBeatService heartBeatService = new HeartBeatServiceImpl(messenger, intervalMs);
        Mockito.doThrow(Exception.class).when(messenger).ping();
        heartBeatService.start();
        Thread.sleep(testRunsMs);
        Mockito.verify(messenger, Mockito.times(1)).ping();
    }
}
