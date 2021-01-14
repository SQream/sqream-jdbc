package com.sqream.jdbc.connector.heartbeat;

/**
 * Should be used in case when sever does not support ping.
 */
public class HeartBeatFakeService implements HeartBeatService {
    @Override
    public void start() {
        /*NOP*/
    }

    @Override
    public void stop() {
        /*NOP*/
    }
}
