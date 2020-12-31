package com.sqream.jdbc.logging;

import com.sqream.jdbc.utils.LoggerUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.logging.*;

public class LoggingService {
    private static final Logger PARENT_LOGGER = Logger.getLogger("com.sqream.jdbc");

    public static Logger getParentLogger() {
        return PARENT_LOGGER;
    }

    public void set(String level, String filePath) {
        removeExistingHandlers();
        if (filePath != null && filePath.length() > 0) {
            setFileHandler(filePath);
        } else {
            setConsoleHandler();
        }
        setLevel(level);
    }

    private void setLevel(String level) {
        if (level == null || level.length() == 0) {
            return;
        }
        switch (LoggerLevel.valueOf(level.toUpperCase())) {
            case OFF:
                PARENT_LOGGER.setLevel(Level.OFF);
                break;
            case DEBUG:
                PARENT_LOGGER.setLevel(Level.FINE);
                break;
            case TRACE:
                PARENT_LOGGER.setLevel(Level.FINEST);
                break;
            default:
                StringJoiner supportedLevels = new StringJoiner(", ");
                Arrays.stream(LoggerLevel.values()).forEach(value -> supportedLevels.add(value.getValue()));
                throw new IllegalArgumentException(String.format(
                        "Unsupported logging level: %s. Driver supports: %s", level, supportedLevels));
        }
    }

    private void setConsoleHandler() {
        ConsoleHandler consoleHandler = new ConsoleHandler();
        consoleHandler.setLevel(Level.ALL);
        consoleHandler.setFormatter(LoggerUtil.getCustomFormatter());
        PARENT_LOGGER.addHandler(consoleHandler);
        PARENT_LOGGER.setLevel(Level.OFF);
    }

    private void setFileHandler(String filePath) {
        try {
            Handler handler = new FileHandler(filePath, true);
            handler.setLevel(Level.ALL);
            handler.setFormatter(LoggerUtil.getCustomFormatter());
            PARENT_LOGGER.addHandler(handler);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeExistingHandlers() {
        for (Handler handler : PARENT_LOGGER.getHandlers()) {
            handler.close();
            PARENT_LOGGER.removeHandler(handler);
        }
    }
}
