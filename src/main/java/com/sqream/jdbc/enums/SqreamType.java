package com.sqream.jdbc.enums;

import com.sqream.jdbc.ColumnMetadata;

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

        this.tid = SqreamTypeId.valueOf(sq_type);

        // this.tid = SqreamTypeId.valueOf(sqreamType.substring(2));
        this.size = typeSize;
    }
}
