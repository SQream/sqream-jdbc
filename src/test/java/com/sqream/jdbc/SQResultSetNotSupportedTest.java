package com.sqream.jdbc;

import org.junit.Test;

import java.io.InputStream;
import java.io.Reader;
import java.sql.*;
import java.util.HashMap;
import java.util.zip.CheckedInputStream;

public class SQResultSetNotSupportedTest {

    private SQResultSet rs = SQResultSet.getInstance(null, null);

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void previousTest() throws SQLException {
        rs.previous();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getStatementTest() throws SQLException {
        rs.getStatement();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBytesByNameTest() throws SQLException {
        rs.getBytes("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBytesByIndexTest() throws SQLException {
        rs.getBytes(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void unwrapTest() throws SQLException {
        rs.unwrap(null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void isWrapperForTest() throws SQLException {
        rs.isWrapperFor(null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getTimeByIndexTest() throws SQLException {
        rs.getTime(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getAsciiStreamByIndexTest() throws SQLException {
        rs.getAsciiStream(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getUnicodeStreamByIndexTest() throws SQLException {
        rs.getUnicodeStream(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBinaryStreamTest() throws SQLException {
        rs.getBinaryStream(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBigDecimal3Test() throws SQLException {
        rs.getBigDecimal("", 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getTimeTest() throws SQLException {
        rs.getTime("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getAsciiStreamTest() throws SQLException {
        rs.getAsciiStream("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getUnicodeStreamTest() throws SQLException {
        rs.getUnicodeStream("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBinaryStream2Test() throws SQLException {
        rs.getBinaryStream("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getCursorNameTest() throws SQLException {
        rs.getCursorName();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getCharacterStreamTest() throws SQLException {
        rs.getCharacterStream(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getCharacterStream2Test() throws SQLException {
        rs.getCharacterStream("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBigDecimal4Test() throws SQLException {
        rs.getBigDecimal("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void isBeforeFirstTest() throws SQLException {
        rs.isBeforeFirst();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void isAfterLastTest() throws SQLException {
        rs.isAfterLast();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void isFirstTest() throws SQLException {
        rs.isFirst();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void isLastTest() throws SQLException {
        rs.isLast();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void beforeFirstTest() throws SQLException {
        rs.beforeFirst();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void afterLastTest() throws SQLException {
        rs.afterLast();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void firstTest() throws SQLException {
        rs.first();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void lastTest() throws SQLException {
        rs.last();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getRowTest() throws SQLException {
        rs.getRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void relativeTest() throws SQLException {
        rs.relative(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void setFetchDirectionTest() throws SQLException {
        rs.setFetchDirection(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getFetchDirectionTest() throws SQLException {
        rs.getFetchDirection();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void setFetchSizeTest() throws SQLException {
        rs.setFetchSize(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getFetchSizeTest() throws SQLException {
        rs.getFetchSize();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getTypeTest() throws SQLException {
        rs.getType();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void rowUpdatedTest() throws SQLException {
        rs.rowUpdated();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void rowInsertedTest() throws SQLException {
        rs.rowInserted();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void rowDeletedTest() throws SQLException {
        rs.rowDeleted();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNullTest() throws SQLException {
        rs.updateNull(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBooleanTest() throws SQLException {
        rs.updateBoolean(0, false);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateByteTest() throws SQLException {
        rs.updateByte(0, (byte) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateShortTest() throws SQLException {
        rs.updateShort(0, (short) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateIntTest() throws SQLException {
        rs.updateInt(0, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateLongTest() throws SQLException {
        rs.updateLong(0, (long) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateFloatTest() throws SQLException {
        rs.updateFloat(0, (float) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateDoubleTest() throws SQLException {
        rs.updateDouble(0, (double) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBigDecimalTest() throws SQLException {
        rs.updateBigDecimal(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateStringTest() throws SQLException {
        rs.updateString(0, "");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBytesTest() throws SQLException {
        rs.updateBytes(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateTimeTest() throws SQLException {
        rs.updateTime(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateTimestampTest() throws SQLException {
        rs.updateTimestamp(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStreamTest() throws SQLException {
        rs.updateAsciiStream(0, null ,0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStreamTest() throws SQLException {
        rs.updateBinaryStream(0, null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStreamTest() throws SQLException {
        rs.updateCharacterStream(0, null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateObjectTest() throws SQLException {
        rs.updateObject(0, null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateObject2Test() throws SQLException {
        rs.updateObject(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNull2Test() throws SQLException {
        rs.updateNull("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBoolean2Test() throws SQLException {
        rs.updateBoolean("", false);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateByte2Test() throws SQLException {
        rs.updateByte("", (byte) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateShort2Test() throws SQLException {
        rs.updateShort("", (short) 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateInt2Test() throws SQLException {
        rs.updateInt("", 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateLong2Test() throws SQLException {
        rs.updateLong("", 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateFloat2Test() throws SQLException {
        rs.updateFloat("", 0f);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateDouble2Test() throws SQLException {
        rs.updateDouble("", 0d);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBigDecimal2Test() throws SQLException {
        rs.updateBigDecimal("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateString2Test() throws SQLException {
        rs.updateString("", "");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBytes2Test() throws SQLException {
        rs.updateBytes("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateDateTest() throws SQLException {
        rs.updateDate("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateTime2Test() throws SQLException {
        rs.updateTime("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateTimestamp2Test() throws SQLException {
        rs.updateTimestamp("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream2Test() throws SQLException {
        rs.updateAsciiStream("", null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream2Test() throws SQLException {
        rs.updateBinaryStream("", null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream2Test() throws SQLException {
        rs.updateCharacterStream("", null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateObject3Test() throws SQLException {
        rs.updateObject("", null, 0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateObject4Test() throws SQLException {
        rs.updateObject("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void insertRowTest() throws SQLException {
        rs.insertRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateRowTest() throws SQLException {
        rs.updateRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void deleteRowTest() throws SQLException {
        rs.deleteRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void refreshRowTest() throws SQLException {
        rs.refreshRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void cancelRowUpdatesTest() throws SQLException {
        rs.cancelRowUpdates();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void moveToInsertRowTest() throws SQLException {
        rs.moveToInsertRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void moveToCurrentRowTest() throws SQLException {
        rs.moveToCurrentRow();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObjectTest() throws SQLException {
        rs.getObject(0, new HashMap<>());
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getRefTest() throws SQLException {
        rs.getRef(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBlobTest() throws SQLException {
        rs.getBlob(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getClobTest() throws SQLException {
        rs.getClob(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getArrayTest() throws SQLException {
        rs.getArray(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObject2Test() throws SQLException {
        rs.getObject("", new HashMap<>());
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getRef2Test() throws SQLException {
        rs.getRef("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getBlob2Test() throws SQLException {
        rs.getBlob("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getClob2Test() throws SQLException {
        rs.getClob("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getArray2Test() throws SQLException {
        rs.getArray("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getTime2Test() throws SQLException {
        rs.getTime(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getTime3Test() throws SQLException {
        rs.getTime("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getURLTest() throws SQLException {
        rs.getURL(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getURL2Test() throws SQLException {
        rs.getURL("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateRefTest() throws SQLException {
        rs.updateRef(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateRef2Test() throws SQLException {
        rs.updateRef("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlobTest() throws SQLException {
        rs.updateBlob(0, (Blob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob2Test() throws SQLException {
        rs.updateBlob("", new CheckedInputStream(null, null));
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob7Test() throws SQLException {
        rs.updateBlob("", (Blob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateArrayTest() throws SQLException {
        rs.updateArray(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateArray2Test() throws SQLException {
        rs.updateArray("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getRowIdTest() throws SQLException {
        rs.getRowId(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getRowId2Test() throws SQLException {
        rs.getRowId("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateRowIdTest() throws SQLException {
        rs.updateRowId(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateRowId2Test() throws SQLException {
        rs.updateRowId("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getHoldabilityTest() throws SQLException {
        rs.getHoldability();
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNStringTest() throws SQLException {
        rs.updateNString(0, "");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNString2Test() throws SQLException {
        rs.updateNString("", "");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClobTest() throws SQLException {
        rs.updateNClob(0, (NClob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClob2Test() throws SQLException {
        rs.updateNClob("", (NClob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNClobTest() throws SQLException {
        rs.getNClob(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNClob2Test() throws SQLException {
        rs.getNClob("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getSQLXMLTest() throws SQLException {
        rs.getSQLXML(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getSQLXML2Test() throws SQLException {
        rs.getSQLXML("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateSQLXMLTest() throws SQLException {
        rs.updateSQLXML(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateSQLXML2Test() throws SQLException {
        rs.updateSQLXML("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNStringTest() throws SQLException {
        rs.getNString(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNString2Test() throws SQLException {
        rs.getNString("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNCharacterStreamTest() throws SQLException {
        rs.getNCharacterStream(0);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getNCharacterStream2Test() throws SQLException {
        rs.getNCharacterStream("");
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStreamTest() throws SQLException {
        rs.updateNCharacterStream(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream2Test() throws SQLException {
        rs.updateNCharacterStream("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream3Test() throws SQLException {
        rs.updateAsciiStream(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream3Test() throws SQLException {
        rs.updateBinaryStream(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream3Test() throws SQLException {
        rs.updateCharacterStream(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream4Test() throws SQLException {
        rs.updateAsciiStream("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream4Test() throws SQLException {
        rs.updateBinaryStream("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream4Test() throws SQLException {
        rs.updateCharacterStream("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob3Test() throws SQLException {
        rs.updateBlob(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob4Test() throws SQLException {
        rs.updateBlob("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClobTest() throws SQLException {
        rs.updateClob(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClob2Test() throws SQLException {
        rs.updateClob("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClo3Test() throws SQLException {
        rs.updateClob(0, (Clob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClob5Test() throws SQLException {
        rs.updateClob("", (Clob) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClob3Test() throws SQLException {
        rs.updateNClob(0, null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClob4Test() throws SQLException {
        rs.updateNClob("", null, 0L);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream3Test() throws SQLException {
        rs.updateNCharacterStream(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNCharacterStream4Test() throws SQLException {
        rs.updateNCharacterStream("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream5Test() throws SQLException {
        rs.updateAsciiStream(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream5Test() throws SQLException {
        rs.updateBinaryStream(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream5Test() throws SQLException {
        rs.updateCharacterStream(0, null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateAsciiStream6Test() throws SQLException {
        rs.updateAsciiStream("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBinaryStream6Test() throws SQLException {
        rs.updateBinaryStream("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateCharacterStream6Test() throws SQLException {
        rs.updateCharacterStream("", null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob5Test() throws SQLException {
        rs.updateBlob(0, (InputStream) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateBlob6Test() throws SQLException {
        rs.updateBlob("", (InputStream) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClob3Test() throws SQLException {
        rs.updateClob(0, (Reader) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateClob4Test() throws SQLException {
        rs.updateClob("", (Reader) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClob5Test() throws SQLException {
        rs.updateNClob(0, (Reader) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void updateNClob6Test() throws SQLException {
        rs.updateNClob("", (Reader) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObject3Test() throws SQLException {
        rs.getObject(0, new HashMap<>());
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObjec4Test() throws SQLException {
        rs.getObject("", new HashMap<>());
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObject5Test() throws SQLException {
        rs.getObject(0, (Class<Object>) null);
    }

    @Test (expected = SQLFeatureNotSupportedException.class)
    public void getObjec6Test() throws SQLException {
        rs.getObject("", (Class<Object>) null);
    }
}
