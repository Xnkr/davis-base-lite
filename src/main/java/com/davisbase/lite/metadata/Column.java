package com.davisbase.lite.metadata;

public class Column {
    private Table table;
    private String columnName;
    private boolean isUnique;
    private boolean isNullable;
    private Short ordinalPosition;
    private boolean isPrimaryKey;
    private DataType dataType;

    public Column(){}

    public Column(String tableName, DataType dataType, String columnName, boolean isUnique, boolean isNullable, short ordinalPosition) {
        this.table = new Table(tableName);
        this.dataType = dataType;
        this.columnName = columnName;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
        this.ordinalPosition = ordinalPosition;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean unique) {
        isUnique = unique;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public Short getOrdinalPosition() {
        return ordinalPosition;
    }

    public void setOrdinalPosition(short ordinalPosition) {
        this.ordinalPosition = ordinalPosition;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "Column{" +
                "table=" + table +
                ", columnName='" + columnName + '\'' +
                ", isUnique=" + isUnique +
                ", isNullable=" + isNullable +
                ", ordinalPosition=" + ordinalPosition +
                ", isPrimaryKey=" + isPrimaryKey +
                ", dataType=" + dataType +
                '}';
    }
}
