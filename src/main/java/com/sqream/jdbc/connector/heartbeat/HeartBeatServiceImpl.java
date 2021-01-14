package com.sqream.jdbc.connector.heartbeat;

import com.sqream.jdbc.connector.messenger.Messenger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HeartBeatServiceImpl implements HeartBeatService {
    private static final Logger LOGGER = Logger.getLogger(HeartBeatServiceImpl.class.getName());

    private final Messenger messenger;
    private final long interval;
    private ScheduledExecutorService executorService;

    public HeartBeatServiceImpl(Messenger messenger, long intervalMs) {
        this.messenger = messenger;
        this.interval = intervalMs;
    }

    @Override
    public void start() {
        if (isRunning()) {
            return;
        }
        executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleWithFixedDelay(() -> {
            try {
                messenger.ping();
            } catch (Exception e) {
                // catch any exception and fail silently
                stop();
                LOGGER.log(Level.WARNING, "Failed to ping server");
            }
        }, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (isRunning()) {
            executorService.shutdownNow();
        }
    }

    private boolean isRunning() {
        return executorService != null && !executorService.isShutdown();
    }
}
