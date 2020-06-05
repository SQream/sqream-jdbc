package com.sqream.jdbc.utils;

import java.text.MessageFormat;
import java.util.Optional;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LoggerUtil {

    public static Formatter getCustomFormatter() {
        return new Formatter() {

            @Override
            public String format(LogRecord record) {

                int threadId = record.getThreadID();
                String threadName = getThread(threadId)
                        .map(Thread::getName)
                        .orElseGet(() -> "Thread with ID " + threadId);

                String shortClassName;
                try {
                    shortClassName = Class.forName(record.getSourceClassName()).getSimpleName();
                } catch (ClassNotFoundException e) {
                    shortClassName = record.getSourceClassName();
                }

                return MessageFormat.format("{0}: {1}#{2}: {3}\n",
                        threadName,  shortClassName, record.getSourceMethodName(), record.getMessage());
            }
        };
    }

    private static Optional<Thread> getThread(long threadId) {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.getId() == threadId)
                .findFirst();
    }
}
