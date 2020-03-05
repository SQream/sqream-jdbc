package com.sqream.jdbc.connector.socket;

import tlschannel.ClientTlsChannel;
import tlschannel.TlsChannel;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;

class SQSocket {
    private static final String TLS_PROTOCOL_VERSION = "TLSv1.2";

    private SocketChannel socketChannel;
    private TlsChannel tlsChannel;  // secure socket

    private String ip;
    private int port;
    private boolean useSsl;

    private SQSocket(String ip, int port, boolean useSsl) throws IOException {
        this.ip = ip;
        this.port = port;
        this.useSsl = useSsl;
        openSocket();
    }

    static SQSocket connect(String ip, int port, boolean useSsl) throws IOException {
        return new SQSocket(ip, port, useSsl);
    }

    int read(ByteBuffer result) throws IOException {
        return (useSsl) ? tlsChannel.read(result) : socketChannel.read(result);
    }

    void write(ByteBuffer data) throws IOException {
        if (useSsl) {
            tlsChannel.write(data);
        } else {
            socketChannel.write(data);
        }
    }

    boolean isOpen() {
        return (useSsl) ? tlsChannel.isOpen() : socketChannel.isOpen();
    }

    void close() throws IOException {
        if (useSsl) {
            if (tlsChannel.isOpen()) {
                tlsChannel.close(); // finish ssl communcication and close SSLEngine
            }
        }
        if (socketChannel.isOpen()) {
            socketChannel.close();
        }
    }

    private void openSocket() {
        try {
            socketChannel = SocketChannel.open();
            socketChannel.connect(new InetSocketAddress(ip, port));
            if (useSsl) {
                SSLContext ssl_context = null;
                ssl_context = SSLContext.getInstance(TLS_PROTOCOL_VERSION);
                ssl_context.init(null,
                        new TrustManager[]{new X509TrustManager() {
                            public X509Certificate[] getAcceptedIssuers() {
                                return null;
                            }

                            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                            }

                            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                            }
                        }},
                        null);
                tlsChannel = ClientTlsChannel.newBuilder(socketChannel, ssl_context).build();

            }
        } catch (IOException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }
}
