package com.davisbase.lite.utils;

import com.davisbase.lite.metadata.DataType;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.Map;

public class DataTypeUtil {
    private static Map<String, DataType> dataTypeStrMap;
    private static Map<Byte, DataType> dataTypeByteMap;
    private static Map<Byte, Integer> dataTypeSizeMap;
    private static Map<DataType, Integer> prettyPrintMap;

    static {
        dataTypeStrMap = new HashMap<>();
        dataTypeByteMap = new HashMap<>();
        dataTypeSizeMap = new HashMap<>();
        prettyPrintMap = new HashMap<>();
    }

    static {
        for (DataType dataType : DataType.values()) {
            dataTypeByteMap.put(dataType.getValue(), dataType);
            dataTypeStrMap.put(dataType.toString(), dataType);

            if (dataType == DataType.TINYINT || dataType == DataType.YEAR) {
                dataTypeSizeMap.put(dataType.getValue(), 1);
                prettyPrintMap.put(dataType, 6);
            } else if (dataType == DataType.SMALLINT) {
                dataTypeSizeMap.put(dataType.getValue(), 2);
                prettyPrintMap.put(dataType, 8);
            } else if (dataType == DataType.INT || dataType == DataType.FLOAT || dataType == DataType.TIME) {
                dataTypeSizeMap.put(dataType.getValue(), 4);
                prettyPrintMap.put(dataType, 10);
            } else if (dataType == DataType.BIGINT || dataType == DataType.DOUBLE
                    || dataType == DataType.DATETIME || dataType == DataType.DATE) {
                dataTypeSizeMap.put(dataType.getValue(), 8);
                prettyPrintMap.put(dataType, 25);
            } else if (dataType == DataType.TEXT) {
                prettyPrintMap.put(dataType, 25);
            } else if (dataType == DataType.NULL) {
                dataTypeSizeMap.put(dataType.getValue(), 0);
                prettyPrintMap.put(dataType, 6);
            }
        }

    }

    public static DataType getDataType(String dataType) {
        return dataTypeStrMap.get(dataType);
    }

    public static DataType getDataType(byte value) {
        if (value > 12)
            return DataType.TEXT;
        return dataTypeByteMap.get(value);
    }

    public static int getPrintOffset(byte value) {
        return prettyPrintMap.get(getDataType(value));
    }

    public static int getPrintOffset(DataType dataType) {
        return prettyPrintMap.get(dataType);
    }

    public static int getLength(DataType type) {
        return getLength(type.getValue());
    }

    public static int getLength(byte value) {
        return getDataType(value) != DataType.TEXT ? dataTypeSizeMap.get(value) : value - 12;
    }

}
