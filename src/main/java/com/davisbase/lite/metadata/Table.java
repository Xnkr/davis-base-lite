package com.davisbase.lite.metadata;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.utils.BPlusTreeImpl;
import com.davisbase.lite.utils.ConditionParser;
import com.davisbase.lite.utils.DataTypeUtil;
import com.davisbase.lite.utils.PageUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Table {
    private String tableName;

    private List<String> columnsStr;
    private List<Column> columns;

    private int rowCount;
    private List<Row> columnData;

    private boolean tableExists;
    private int rootPageNo;

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int recordCount) {
        this.rowCount = recordCount;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public boolean isTableExists() {
        return tableExists;
    }

    public int getRootPageNo() {
        return rootPageNo;
    }

    public int getLastRowId() {
        return lastRowId;
    }

    private int lastRowId;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Table() {
        this.tableName = "";
    }

    public Table(String tableName) {
        this.tableName = tableName;
        loadTable();
    }

    public List<String> getColumnNames() {
        return columnsStr;
    }

    public void loadTable() {
        tableExists = false;
        try {

            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    DavisBaseBinaryFile.getTablePath(DavisBaseBinaryFile.davisbaseTables), "r");

            int rootPageNo = PageUtil.getRootPageNo(davisbaseTablesCatalog);

            isTableExists(davisbaseTablesCatalog, rootPageNo);

            davisbaseTablesCatalog.close();
            if (tableExists) {
                loadColumnData();
            } else {
                System.out.println("Table does not exist.");
            }

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void loadColumnData() {
        try {

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                    DavisBaseBinaryFile.getTablePath(DavisBaseBinaryFile.davisbaseColumns), "r");
            int rootPageNo = PageUtil.getRootPageNo(davisbaseColumnsCatalog);

            columnData = new ArrayList<>();
            columns = new ArrayList<>();
            columnsStr = new ArrayList<>();
            BPlusTreeImpl bPlusOneTree = new BPlusTreeImpl(davisbaseColumnsCatalog, rootPageNo, tableName);

            for (Integer pageNo : bPlusOneTree.getAllLeaves()) {
                PageUtil page = new PageUtil(davisbaseColumnsCatalog, pageNo);
                for (Row record : page.getPageRecords()) {
                    Table table = new Table();
                    if (record.getAttributes().get(0).fieldValueStr.equals(tableName)) {
                        columnData.add(record);
                        columnsStr.add(record.getAttributes().get(1).fieldValueStr);
                        Column column = new Column();
                        table.setTableName(tableName);
                        column.setTable(table);
                        column.setDataType(DataTypeUtil.getDataType(record.getAttributes().get(2).fieldValueStr));
                        column.setColumnName(record.getAttributes().get(1).fieldValueStr);
                        column.setUnique(record.getAttributes().get(6).fieldValueStr.equals("YES"));
                        column.setNullable(record.getAttributes().get(4).fieldValueStr.equals("YES"));
                        column.setOrdinalPosition(Short.parseShort(record.getAttributes().get(3).fieldValueStr));
                        column.setPrimaryKey(record.getAttributes().get(5).fieldValueStr.equals("PRI"));
                        columns.add(column);
                    }
                }
            }

            davisbaseColumnsCatalog.close();
        } catch (Exception e) {
            System.out.println("Exception while getting column data for " + tableName);
        }

    }

    private void isTableExists(RandomAccessFile davisbaseTablesCatalog, int rootPageNo) throws IOException {
        BPlusTreeImpl bplusOneTree = new BPlusTreeImpl(davisbaseTablesCatalog, rootPageNo, tableName);
        for (Integer pageNo : bplusOneTree.getAllLeaves()) {
            PageUtil pageType = new PageUtil(davisbaseTablesCatalog, pageNo);

            for (Row record : pageType.getPageRecords()) {

                if (record.getAttributes().get(0).fieldValueStr.equals(tableName)) {
                    this.rootPageNo = Integer.parseInt(record.getAttributes().get(3).fieldValueStr);
                    rowCount = Integer.parseInt(record.getAttributes().get(1).fieldValueStr);
                    tableExists = true;
                    break;
                }
            }
            if (tableExists)
                break;
        }
    }

    public boolean validateInsert(List<Attribute> row) throws IOException {
        RandomAccessFile tableFile = new RandomAccessFile(DavisBaseBinaryFile.getTablePath(tableName), "r");
        DavisBaseBinaryFile file = new DavisBaseBinaryFile(tableFile);


        for (int i = 0; i < columns.size(); i++) {

            ConditionParser condition = new ConditionParser(columns.get(i).getDataType());
            condition.setColumName(columns.get(i).getColumnName());
            condition.columnOrdinal = i;
            condition.setOperator("=");

            if (columns.get(i).isUnique()) {
                condition.setConditionValue(row.get(i).fieldValueStr);
                if (file.rowExists(this, columns.get(i).getColumnName(), condition)) {
                    System.out.println("Insert failed: Column " + columns.get(i).getColumnName() + " should be unique.");
                    tableFile.close();
                    return false;
                }
            }
        }
        tableFile.close();
        return true;
    }

    @Override
    public String toString() {
        return "Table{" + tableName + '}';
    }

    public void updateMetaData() {
        try {
            RandomAccessFile tableFile = new RandomAccessFile(DavisBaseBinaryFile.getTablePath(tableName), "r");

            int rootPageNo = PageUtil.getRootPageNo(tableFile);
            tableFile.close();


            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    DavisBaseBinaryFile.getTablePath(DavisBaseBinaryFile.davisbaseTables), "rw");

            DavisBaseBinaryFile tablesBinaryFile = new DavisBaseBinaryFile(davisbaseTablesCatalog);

            Table table = new Table(DavisBaseBinaryFile.davisbaseTables);

            ConditionParser condition = new ConditionParser(DataType.TEXT);
            condition.setColumName("table_name");
            condition.columnOrdinal = 0;
            condition.setConditionValue(tableName);
            condition.setOperator("=");

            List<String> columns = Arrays.asList("record_count", "root_page");
            List<String> newValues = new ArrayList<>();

            newValues.add(Integer.toString(rowCount));
            newValues.add(Integer.toString(rootPageNo));

            tablesBinaryFile.updateRows(table, condition, columns, newValues);

            davisbaseTablesCatalog.close();
        } catch (IOException e) {
            System.out.println("Exception updating meta data for " + tableName);
        }
    }

    public List<Integer> getOrdinalPostions(List<String> columns) {
        List<Integer> ordinalPostions = new ArrayList<>();
        for (String column : columns) {
            ordinalPostions.add(columnsStr.indexOf(column));
        }
        return ordinalPostions;
    }
}
