package com.davisbase.lite.metadata;

import com.davisbase.lite.utils.ByteUtil;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Attribute {

    public DataType dataType;
    public Byte[] fieldValueByte;
    public byte[] fieldValuebyte;
    public String fieldValueStr;

    Attribute(DataType dataType, byte[] fieldValue) {
        this.dataType = dataType;
        try {
            setAttrValue(dataType, fieldValue);
            this.fieldValuebyte = fieldValue;
        } catch (Exception ex) {
            System.out.println("Exception while formatting");
        }

    }

    private void setAttrValue(DataType dataType, byte[] fieldValue) {
        switch (dataType) {
            case NULL:
                this.fieldValueStr = "NULL";
                break;
            case TINYINT:
                this.fieldValueStr = Byte.valueOf(ByteUtil.byteFromByteArray(fieldValue)).toString();
                break;
            case SMALLINT:
                this.fieldValueStr = Short.valueOf(ByteUtil.shortFromByteArray(fieldValue)).toString();
                break;
            case INT:
                this.fieldValueStr = Integer.valueOf(ByteUtil.intFromByteArray(fieldValue)).toString();
                break;
            case BIGINT:
                this.fieldValueStr = Long.valueOf(ByteUtil.longFromByteArray(fieldValue)).toString();
                break;
            case FLOAT:
                this.fieldValueStr = Float.valueOf(ByteUtil.floatFromByteArray(fieldValue)).toString();
                break;
            case DOUBLE:
                this.fieldValueStr = Double.valueOf(ByteUtil.doubleFromByteArray(fieldValue)).toString();
                break;
            case YEAR:
                this.fieldValueStr = Integer.valueOf((int) ByteUtil.byteFromByteArray(fieldValue) + 2000).toString();
                break;
            case TIME:
                int millisSinceMidnight = ByteUtil.intFromByteArray(fieldValue) % 86400000;
                int seconds = millisSinceMidnight / 1000;
                int hours = seconds / 3600;
                int remHourSeconds = seconds % 3600;
                int minutes = remHourSeconds / 60;
                int remSeconds = remHourSeconds % 60;
                this.fieldValueStr = String.format("%02d", hours) + ":" + String.format("%02d", minutes) + ":" + String.format("%02d", remSeconds);
                break;
            case DATETIME:
                Date rawdatetime = new Date(ByteUtil.longFromByteArray(fieldValue));
                this.fieldValueStr = String.format("%02d", rawdatetime.getYear() + 1900) + "-" + String.format("%02d", rawdatetime.getMonth() + 1)
                        + "-" + String.format("%02d", rawdatetime.getDate()) + "_" + String.format("%02d", rawdatetime.getHours()) + ":"
                        + String.format("%02d", rawdatetime.getMinutes()) + ":" + String.format("%02d", rawdatetime.getSeconds());
                break;
            case DATE:
                Date rawdate = new Date(ByteUtil.longFromByteArray(fieldValue));
                this.fieldValueStr = String.format("%02d", rawdate.getYear() + 1900) + "-" + String.format("%02d", rawdate.getMonth() + 1)
                        + "-" + String.format("%02d", rawdate.getDate());
                break;
            default:
                this.fieldValueStr = new String(fieldValue, StandardCharsets.UTF_8);
                break;
        }
    }

    public Attribute(DataType dataType, String fieldValue) throws Exception {
        this.dataType = dataType;
        this.fieldValueStr = fieldValue;

        try {
            setAttrValueFromStr(dataType, fieldValue);
            this.fieldValueByte = ByteUtil.byteToBytes(fieldValuebyte);
        } catch (Exception e) {
            System.out.println("Cannot convert " + fieldValue + " to " + dataType.toString());
            throw e;
        }
    }

    private void setAttrValueFromStr(DataType dataType, String fieldValue) throws ParseException {
        switch (dataType) {
            case NULL:
                this.fieldValuebyte = null;
                break;
            case TINYINT:
                this.fieldValuebyte = new byte[]{Byte.parseByte(fieldValue)};
                break;
            case SMALLINT:
                this.fieldValuebyte = ByteUtil.shortTobytes(Short.parseShort(fieldValue));
                break;
            case INT:
            case TIME:
                this.fieldValuebyte = ByteUtil.intTobytes(Integer.parseInt(fieldValue));
                break;
            case BIGINT:
                this.fieldValuebyte = ByteUtil.longTobytes(Long.parseLong(fieldValue));
                break;
            case FLOAT:
                this.fieldValuebyte = ByteUtil.floatTobytes(Float.parseFloat(fieldValue));
                break;
            case DOUBLE:
                this.fieldValuebyte = ByteUtil.doubleTobytes(Double.parseDouble(fieldValue));
                break;
            case YEAR:
                this.fieldValuebyte = new byte[]{(byte) (Integer.parseInt(fieldValue) - 2000)};
                break;
            case DATETIME:
                SimpleDateFormat sdftime = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
                Date datetime = sdftime.parse(fieldValue);
                this.fieldValuebyte = ByteUtil.longTobytes(datetime.getTime());
                break;
            case DATE:
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = sdf.parse(fieldValue);
                this.fieldValuebyte = ByteUtil.longTobytes(date.getTime());
                break;
            case TEXT:
                this.fieldValuebyte = fieldValue.getBytes();
                break;
            default:
                this.fieldValuebyte = fieldValue.getBytes(StandardCharsets.US_ASCII);
                break;
        }
    }


}
