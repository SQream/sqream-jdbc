package com.sqream.jdbc.connector.heartbeat;

import com.sqream.jdbc.connector.ServerVersion;
import com.sqream.jdbc.connector.messenger.Messenger;

public class HeartBeatServiceFactory {
    private static final String PING_SUPPORTED_VERSION = "2020.3.1";
    private static final Long PING_INTERVAL_MS = 10_000L;

    public static HeartBeatService getService(String serverVersion, Messenger messenger) {
        if (serverVersion != null && serverVersion.length() > 0 &&
                ServerVersion.compare(serverVersion, PING_SUPPORTED_VERSION) >= 0) {
            return new HeartBeatServiceImpl(messenger, PING_INTERVAL_MS);
        }
        return new HeartBeatFakeService();
    }
}
