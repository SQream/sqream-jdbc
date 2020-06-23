package com.sqream.jdbc;

import com.sqream.jdbc.enums.SqreamType;
import com.sqream.jdbc.enums.SqreamTypeId;

public class ColumnMetadata {
	private String  name;

	private SqreamType type;
	/**
	 * Indicates the designated column's normal maximum width in characters.
	 */
	private int displaySize;
	private Boolean isNul;
    
	public ColumnMetadata(String _name, String _type, int _size, boolean _is_nullable) {
		name = 			_name;   
		type = 			new SqreamType(_type, _size);
		isNul = 		_is_nullable;
	}


	public String getName() {
		return name;
	}

	public SqreamTypeId getTypeId() {
		return type.tid;
	}

	public SqreamType getType() {
		return type;
	}

	public Boolean isNull() {
		return this.isNul;
	}
}
