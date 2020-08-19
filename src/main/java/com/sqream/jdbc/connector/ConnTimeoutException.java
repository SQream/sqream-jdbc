package com.sqream.jdbc.connector;

public class ConnTimeoutException extends ConnException {

    private static final long serialVersionUID = 1L;

    public ConnTimeoutException(String message) {
        super(message);
    }

    public ConnTimeoutException(Throwable e) {
        super(e);
    }
}
