/**
 * 
 */
package com.sqream.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.sql.Types;


/**
 * @author root
 *
 */
public enum SqrmTypes {
	ftByte		//signed int8
	{
		@Override
		public int getSize() {
			return Byte.SIZE/8;
		}

		@Override
		public Object getObj (byte[] value, int offset, int len) {
			return value[offset];
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.get();
		}

		@Override
		public Object getNullValue() {
			return (byte)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "TINYINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.TINYINT;
		}
	},
	ftBool		//signed int8
	{
		@Override
		public int getSize() {
			return Byte.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			return value[offset];
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.get();
		}

		@Override
		public Object getNullValue() {
			return null;
		}
		
		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "BOOLEAN";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.BOOLEAN;
		}
	},
	ftUByte		//unsigned int8
	{
		@Override
		public int getSize() {
			return Byte.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			return value[offset];
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.get();
		}

		@Override
		public Object getNullValue() {
			return (byte)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "TINYINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.TINYINT;
		}
	},
	ftShort		//signed int16
	{
		@Override
		public int getSize() {
			return Short.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getShort();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getShort();
		}
		@Override
		public Object getNullValue() {
			return (short)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "SMALLINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.SMALLINT;
		}
	},
	ftUShort	//unsigned int16
	{
		@Override
		public int getSize() {
			return Short.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getShort();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getShort();
		}
		@Override
		public Object getNullValue() {
			return (short)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "SMALLINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.SMALLINT;
		}
	},  
	ftInt		//signed int32
	{
		@Override
		public int getSize() {
			return Integer.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getInt();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getInt();
		}
		@Override
		public Object getNullValue() {
			return (Integer)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "int";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.INTEGER;
		}
	},
	ftUInt		//unsigned int32
	{
		@Override
		public int getSize() {
			return Integer.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getInt();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getInt();
		}

		@Override
		public Object getNullValue() {
			return (Integer)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "int";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.INTEGER;
		}
	},
	ftLong		//signed int64
	{
		@Override
		public int getSize() {
			return Long.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getLong();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getLong();
		}
		@Override
		public Object getNullValue() {
			return (long)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "BIGINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.BIGINT;
		}
	},
	ftULong		//unsigned int64
	{
		@Override
		public int getSize() {
			return Long.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getLong();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getNullValue() {
			return (long)0;
		}
		
		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "BIGINT";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.BIGINT;
		}
	},  
	ftFloat		//float32
	{
		@Override
		public int getSize() {
			return Float.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getFloat();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getFloat();
		}
		@Override
		public Object getNullValue() {
			return (float)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "REAL";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.REAL;
		}
	},
	ftDouble 	//float64		 
	{
		@Override
		public int getSize() {
			return Double.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getDouble();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getDouble();
		}
		@Override
		public Object getNullValue() {
			return (double)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "DOUBLE";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.DOUBLE;
		}
	},
	ftVarchar	//character string with variable length
	{
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			// TODO Auto-generated method stub
			return new String(value, offset, len);
			
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return new String(bb.asCharBuffer().array());
		}
		@Override
		public Object getNullValue() {
			//return (String)"";
			return ""; 
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "VARCHAR";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.VARCHAR;
		}
	},
	ftChar	//character string with variable length
	{
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			// TODO Auto-generated method stub
			return new String(value, offset, len);
			
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return new String(bb.asCharBuffer().array());
		}
		@Override
		public Object getNullValue() {
			return (String)"";
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "CHAR";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.CHAR;
		}
	},  
	
	ftDate		//date
	{
		public java.sql.Date ExtractDate(int dateNum)
		{
			return DatetimeLogic.dateFromInt(dateNum, false) ;
			
		}
		
		@Override
		public int getSize() {
			return Integer.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return ExtractDate(bb.getInt());
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return ExtractDate(bb.getInt());
		}
		@Override
		public Object getNullValue() {
			//java.sql.Date jsd = new java.sql.Date(0, 0, 0);
			java.sql.Date jsd = null;
			return jsd;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "DATE";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.DATE;
		}
	},  
	ftTime		//time
	{
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return bb.getLong();
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			return bb.getLong();
		}
		@Override
		public Object getNullValue() {
			return (Integer)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "TIME";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.TIME;
		}
	},
	ftDateTime //Date/Time stamp
	{
		public java.sql.Date ExtractDate(int dateNum)
		{
			return DatetimeLogic.dateFromInt(dateNum, true) ;	
			
		}
		
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return Long.SIZE/8;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			// TODO Auto-generated method stub
			ByteBuffer bb = ByteBuffer.wrap(value, offset, len);
			bb.order(ByteOrder.LITTLE_ENDIAN);
			return ExtractDate(bb.getInt());
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return ExtractDate(bb.getInt());
		}
		@Override
		public Object getNullValue() {
			return (Integer)0;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "TIMESTAMP";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.TIMESTAMP;
		}
	},
	ftBlob		//binary large object with variable size
	{
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "NVARCHAR";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return Types.NVARCHAR;
		}
	},
	ftNone
	{
		@Override
		public int getSize() {
			// TODO Auto-generated method stub
			return -1;
		}

		@Override
		public Object getObj(byte[] value, int offset, int len) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Object getObj(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getSqlTypeName() {
			// TODO Auto-generated method stub
			return "WHATDOINEEDSUBTITLEFOR";
		}

		@Override
		public int getSqlType() {
			// TODO Auto-generated method stub
			return 0;
		}
	}
	;
	public static SqrmTypes getTypeByStr(String typeStr)
	{
		return SqrmTypes.valueOf(typeStr); 
	}
	public Object getNullValue() {
		// TODO Auto-generated method stub
		return null;
	}
	public static SqrmTypes getType(int code) {
		if (SqrmTypes.ftByte.ordinal() == code) {
			return SqrmTypes.ftByte;
		}
		if (SqrmTypes.ftUByte.ordinal() == code) {
			return SqrmTypes.ftUByte;
		}
		if (SqrmTypes.ftShort.ordinal() == code) {
			return SqrmTypes.ftShort;
		}
		if (SqrmTypes.ftUShort.ordinal() == code) {
			return SqrmTypes.ftUShort;
		}
		if (SqrmTypes.ftInt.ordinal() == code) {
			return SqrmTypes.ftInt;
		}
		if (SqrmTypes.ftUInt.ordinal() == code) {
			return SqrmTypes.ftUInt;
		}
		if (SqrmTypes.ftLong.ordinal() == code) {
			return SqrmTypes.ftLong;
		}
		if (SqrmTypes.ftULong.ordinal() == code) {
			return SqrmTypes.ftLong;
		}
		if (SqrmTypes.ftFloat.ordinal() == code) {
			return SqrmTypes.ftFloat;
		}
		if (SqrmTypes.ftDouble.ordinal() == code) {
			return SqrmTypes.ftDouble;
		}
		if (SqrmTypes.ftVarchar.ordinal() == code) {
			return SqrmTypes.ftVarchar;
		}
		if (SqrmTypes.ftDate.ordinal() == code) {
			return SqrmTypes.ftDate;
		}
		if (SqrmTypes.ftTime.ordinal() == code) {
			return SqrmTypes.ftTime;
		}
		if (SqrmTypes.ftDateTime.ordinal() == code) {
			return SqrmTypes.ftDateTime;
		}
		if (SqrmTypes.ftBlob.ordinal() == code) {
			return SqrmTypes.ftBlob;
		}
		
		return SqrmTypes.ftNone;
	}
	
	public abstract int getSize();
	public abstract Object getObj(byte[] value, int offset, int len);
	public abstract Object getObj(ByteBuffer bb);
	public abstract String getSqlTypeName();
	public abstract int getSqlType();
	

	public static int getSqlTypeBySqreamType(String sqreamType)
	{
		SqrmTypes st = SqrmTypes.valueOf(sqreamType);
		//System.out.println("getSqlTypeBySqreamType = "+st+" retVal="+st.getSqlType());
		return st.getSqlType();
	}

	/**
	 * in use when insert into (handling NULLS)
	 * @param sqreamType
	 * @return
	 * @throws SQLException 
	 */
	static SqrmTypes getSqreamTypeBySqlType(int sqlType) throws SQLException
	{
		SqrmTypes rc= null;  
		for (SqrmTypes st : SqrmTypes.values())
		{
			if(st.getSqlType()==sqlType)
			{
				rc = st;
				break;
			}
		}
		if(rc==null)
		{
			if (sqlType==Types.DECIMAL) // HAAAAACK FOR PENTAHO !!!!!!!
			{
				
				rc = SqrmTypes.ftFloat;
			}
			else
				throw new SQLException("getSqreamTypeBySqlTypeError: unsupported sqlType ("+sqlType+")");
		}
		return rc;
	}
	
	public static String getSqlTypeNameBySqreamType(String sqreamType) throws SQLException
	{
		SqrmTypes st = SqrmTypes.valueOf(sqreamType);
		
		//System.out.println("getSqlTypeBySqreamType = "+st+" retVal = "+getSqlTypeNameBySqlType(st.getSqlType()));
		return getSqlTypeNameBySqlType(st.getSqlType());
	}

       public static String getSqlTypeNameBySqlType(int sqlType) throws SQLException
        {
            String rc = "Undefined Sql Type";
            java.lang.reflect.Field[] fields = java.sql.Types.class.getFields();
            for (int i = 0; i < fields.length; i++)
            {
                Integer value;
                try {
                    value = (Integer) fields[i].get(null);
                } catch(java.lang.IllegalAccessException e) {
                    // OMG sqream has BROKEN THE LAW!!!!
                	throw new SQLException(e);
                }
                if (value.equals(sqlType))
                {
                    switch(sqlType) {
                    case Types.TINYINT:
                        return "tinyint";
                    case Types.BOOLEAN:
                        return "bit";
                    case Types.SMALLINT:
                        return "smallint";
                    case Types.INTEGER:
                        return "int";
                    case Types.BIGINT:
                        return "bigint";
                    case Types.REAL:
                        return "real";
                    case Types.DOUBLE:
                        return "float";
                    case Types.VARCHAR:
                        return "varchar";
                    case Types.DATE:
                        return "date";
                    case Types.TIMESTAMP:
                        return "timestamp";
                    case Types.BLOB:          // 
                    	return "nvarchar";
                    case Types.NVARCHAR:
                    	return "nvarchar";	
                    default:
                        throw new RuntimeException("unrecognised type new: " + sqlType);
                    }
                    /*System.out.println("Found SqlType for "+value);
                      rc = fields[i].getName();
                      break;*/
                }
            }
            return rc;
        }
}


