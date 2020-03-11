package com.sqream.jdbc.connector.socket;

import com.sqream.jdbc.connector.ConnException;
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

public class SQSocket {
    private static final String TLS_PROTOCOL_VERSION = "TLSv1.2";

    private SocketChannel socketChannel;
    private TlsChannel tlsChannel;  // secure socket

    private String ip;
    private int port;
    private boolean useSsl;

    private SQSocket(String ip, int port, boolean useSsl) throws ConnException {
        this.ip = ip;
        this.port = port;
        this.useSsl = useSsl;
        openSocket();
    }

    public static SQSocket connect(String ip, int port, boolean useSsl) throws ConnException {
        return new SQSocket(ip, port, useSsl);
    }

    public int read(ByteBuffer result) throws ConnException {
        try {
            return (useSsl) ? tlsChannel.read(result) : socketChannel.read(result);
        } catch (IOException e) {
            throw new ConnException(e);
        }
    }

    void write(ByteBuffer data) throws ConnException {
        try {
            if (useSsl) {
                tlsChannel.write(data);
            } else {
                socketChannel.write(data);
            }
        } catch (IOException e) {
            throw new ConnException(e);
        }
    }

    boolean isOpen() {
        return (useSsl) ? tlsChannel.isOpen() : socketChannel.isOpen();
    }

    public void close() throws ConnException {
        try {
            if (useSsl) {
                if (tlsChannel.isOpen()) {
                    tlsChannel.close(); // finish ssl communcication and close SSLEngine
                }
            }
            if (socketChannel.isOpen()) {
                socketChannel.close();
            }
        } catch (IOException e) {
            throw new ConnException(e);
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
