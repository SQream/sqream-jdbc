package com.sqream.jdbc.enums;

import java.sql.Types;

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