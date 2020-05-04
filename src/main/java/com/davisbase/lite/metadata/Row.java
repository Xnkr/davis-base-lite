package com.davisbase.lite.metadata;

import com.davisbase.lite.utils.ByteUtil;
import com.davisbase.lite.utils.DataTypeUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Row {
    private int rowId;
    private Byte[] dataTypes;
    private Byte[] recordBody;
    private List<Attribute> attributes;
    private short recordOffset;
    private short pageHeaderIndex;

    public int getRowId() {
        return rowId;
    }

    public Byte[] getDataTypes() {
        return dataTypes;
    }

    public short getRecordOffset() {
        return recordOffset;
    }

    public short getPageHeaderIndex() {
        return pageHeaderIndex;
    }

    public Row(short pageHeaderIndex, int rowId, short recordOffset, byte[] colDatatypes, byte[] recordBody)
    {
        this.rowId = rowId;
        this.recordBody= ByteUtil.byteToBytes(recordBody);
        this.dataTypes = ByteUtil.byteToBytes(colDatatypes);
        this.recordOffset =  recordOffset;
        this.pageHeaderIndex = pageHeaderIndex;
        setAttributes();
    }

    public List<Attribute> getAttributes()
    {
        return attributes;
    }

    private void setAttributes()
    {
        attributes = new ArrayList<>();
        int pointer = 0;
        for(Byte colDataType : dataTypes)
        {
            byte[] fieldValue = ByteUtil.Bytestobytes(Arrays.copyOfRange(recordBody,pointer, pointer + DataTypeUtil.getLength(colDataType)));
            attributes.add(new Attribute(DataTypeUtil.getDataType(colDataType), fieldValue));
            pointer =  pointer + DataTypeUtil.getLength(colDataType);
        }
    }
}
