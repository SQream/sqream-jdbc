package com.sqream.jdbc.connector;

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

    private SocketChannel s = SocketChannel.open();
    private TlsChannel ss;  // secure socket
    private SSLContext ssl_context = SSLContext.getDefault();

    private String ip;
    private int port;
    private boolean useSsl = false;

    SQSocket(String ip, int port) throws IOException, NoSuchAlgorithmException, KeyManagementException {
        this.ip = ip;
        this.port = port;
        ssl_context = SSLContext.getInstance("TLSv1.2");
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
    }

    void connect(boolean useSsl) throws IOException {
        this.useSsl = useSsl;
        s.connect(new InetSocketAddress(ip, port));
        if (useSsl) {
            ss = ClientTlsChannel.newBuilder(s, ssl_context).build();
        }
    }

    void close() throws IOException {
        if (useSsl) {
            if (ss.isOpen()) {
                ss.close(); // finish ssl communcication and close SSLEngine
            }
        }
        if (s.isOpen()) {
            s.close();
        }
    }

    void reconnect(String ip, int port, boolean useSsl) throws IOException {
        this.s = SocketChannel.open();
        this.ip = ip;
        this.port = port;
        this.connect(useSsl);
    }

    int read(ByteBuffer result) throws IOException {
        return (useSsl) ? ss.read(result) : s.read(result);
    }

    void write(ByteBuffer data) throws IOException {
        if (useSsl) {
            ss.write(data);
        } else {
            s.write(data);
        }
    }

    boolean isOpen() {
        return (useSsl) ? ss.isOpen() : s.isOpen();
    }
}
