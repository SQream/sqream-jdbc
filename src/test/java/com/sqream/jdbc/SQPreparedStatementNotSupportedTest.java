package com.sqream.jdbc;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.*;

import static com.sqream.jdbc.TestEnvironment.createConnection;

public class SQPreparedStatementNotSupportedTest {
    private static SQPreparedStatement pstmt;

    @BeforeClass
    public static void setUp() throws SQLException {
        try (Connection conn = createConnection();
             PreparedStatement preparedStatement = conn.prepareStatement("select 1")) {

            pstmt = (SQPreparedStatement) preparedStatement;
        }
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void addBatch() throws SQLException {
        pstmt.addBatch("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeQuery() throws SQLException {
        pstmt.executeQuery("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getConnection() throws SQLException {
        pstmt.getConnection();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getGeneratedKeys() throws SQLException {
        pstmt.getGeneratedKeys();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getWarnings() throws SQLException {
        pstmt.getWarnings();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCursorName() throws SQLException {
        pstmt.setCursorName("");
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setEscapeProcessing() throws SQLException {
        pstmt.setEscapeProcessing(false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirection() throws SQLException {
        pstmt.setFetchDirection(0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setMaxFieldSize() throws SQLException {
        pstmt.setMaxFieldSize(0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setPoolable() throws SQLException {
        pstmt.setPoolable(false);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void unwrap() throws SQLException {
        pstmt.unwrap(null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void clearParameters() throws SQLException {
        pstmt.clearParameters();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void executeUpdate() throws SQLException {
        pstmt.executeUpdate();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setArray() throws SQLException {
        pstmt.setArray(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream() throws SQLException {
        pstmt.setAsciiStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream2() throws SQLException {
        pstmt.setAsciiStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setAsciiStream3() throws SQLException {
        pstmt.setAsciiStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBigDecimal() throws SQLException {
        pstmt.setBigDecimal(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream() throws SQLException {
        pstmt.setBinaryStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        });
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStream2() throws SQLException {
        pstmt.setBinaryStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBinaryStrea3() throws SQLException {
        pstmt.setBinaryStream(0, new InputStream() {
            @Override
            public int read() throws IOException {
                return 0;
            }
        }, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob() throws SQLException {
        pstmt.setBlob(0, (Blob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob2() throws SQLException {
        pstmt.setBlob(0, (InputStream) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBlob3() throws SQLException {
        pstmt.setBlob(0, (InputStream) null, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setBytes() throws SQLException {
        pstmt.setBytes(0, new byte[0]);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream() throws SQLException {
        pstmt.setCharacterStream(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream2() throws SQLException {
        pstmt.setCharacterStream(0, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCharacterStream3() throws SQLException {
        pstmt.setCharacterStream(0, null, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob() throws SQLException {
        pstmt.setClob(0, (Clob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob2() throws SQLException {
        pstmt.setClob(0, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setClob3() throws SQLException {
        pstmt.setClob(0, null, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream() throws SQLException {
        pstmt.setNCharacterStream(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNCharacterStream2() throws SQLException {
        pstmt.setNCharacterStream(0, null, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob() throws SQLException {
        pstmt.setNClob(0, (NClob) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob2() throws SQLException {
        pstmt.setNClob(0, (Reader) null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setNClob3() throws SQLException {
        pstmt.setNClob(0, null, 0L);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setObject() throws SQLException {
        pstmt.setObject(0, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setObject2() throws SQLException {
        pstmt.setObject(0, null, 0, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRef() throws SQLException {
        pstmt.setRef(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setRowId() throws SQLException {
        pstmt.setRowId(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setSQLXML() throws SQLException {
        pstmt.setSQLXML(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTime() throws SQLException {
        pstmt.setTime(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setTime2() throws SQLException {
        pstmt.setTime(0, null, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setURL() throws SQLException {
        pstmt.setURL(0, null);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setUnicodeStream() throws SQLException {
        pstmt.setUnicodeStream(0, null, 0);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void closeOnCompletion() throws SQLException {
        pstmt.closeOnCompletion();
    }

}
