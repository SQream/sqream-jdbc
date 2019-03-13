package com.sqream.jdbc;

import java.sql.Types;


public class ColumnMetadata {
	

	public enum SqreamTypeId {
		Bool(Types.BOOLEAN),
		Tinyint(Types.TINYINT),
		Smallint(Types.SMALLINT),
		Int(Types.INTEGER),
		Bigint(Types.BIGINT),
		Real(Types.REAL),
		Float(Types.DOUBLE),
		
		Date(Types.DATE),
		DateTime(Types.TIMESTAMP),
		Varchar(Types.VARCHAR),
		NVarchar(Types.NVARCHAR);
		
		private int value;
		
		SqreamTypeId(int value) {
			this.value = value;
		}
		
		public int getValue(){
			return value;
		}
		
	}
	
	public class SqreamType {
		public SqreamTypeId tid;
		public int size;
		
		public SqreamType(String sqreamType, int typeSize) {
			String sq_type = sqreamType.substring(2);
			if(sq_type.equals("UByte"))
				sq_type = "Tinyint";
			else if(sq_type.equals("Short"))
				sq_type = "Smallint";
			else if(sq_type.equals("Long"))
				sq_type = "Bigint";
			else if(sq_type.equals("Float"))
				sq_type = "Real";
			else if(sq_type.equals("Double"))
				sq_type = "Float";
			else if(sq_type.equals("Blob"))
				sq_type = "NVarchar";
			
			this.tid =  SqreamTypeId.valueOf(sq_type);
			
			// this.tid = SqreamTypeId.valueOf(sqreamType.substring(2));
			this.size = typeSize;
		}
	}

	public String  name;
	//public String  type;   
	public SqreamType type;  
	int	   typeSize;	// no modifier = available in class and package						
    public Boolean isNul;
	
    
	public ColumnMetadata(String _name, String _type, int _size, boolean _is_nullable) {
		name = 			_name;   
		type = 			new SqreamType(_type, _size);
		isNul = 		_is_nullable;
	}
	
	
}
