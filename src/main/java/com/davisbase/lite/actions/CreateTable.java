package com.davisbase.lite.actions;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.metadata.*;
import com.davisbase.lite.utils.BPlusTreeImpl;
import com.davisbase.lite.utils.DataTypeUtil;
import com.davisbase.lite.utils.PageUtil;
import com.davisbase.lite.utils.DavisBaseUtil;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CreateTable {

    static Table table;
    static String primaryKeyColumn = "";
    static List<Column> columns = new ArrayList<>();

    private static void parseColumns(String createTableString) {
        columns.clear();
        String rawColumnsStr = createTableString.substring
                (createTableString.indexOf("(") + 1, createTableString.length() - 1);

        String[] rawColumnsArr = rawColumnsStr.split(",");
        List<String> rawColumns = new ArrayList<>(Arrays.asList(rawColumnsArr));

        short ordinalPosition = 1;

        for (String rawColumn : rawColumns) {
            List<String> rawColumnTokens = new ArrayList<>(Arrays.asList(rawColumn.trim().split(" ")));
            Column column = new Column();
            column.setTable(table);
            column.setColumnName(rawColumnTokens.get(0));
            column.setDataType(DataTypeUtil.getDataType(rawColumnTokens.get(1).toUpperCase()));
            column.setNullable(!rawColumn.contains("not null"));
            column.setUnique(rawColumn.contains("unique"));
            if (rawColumn.contains("primary key")) {
                column.setPrimaryKey(true);
                column.setUnique(true);
                column.setNullable(false);
                primaryKeyColumn = column.getColumnName();
            }
            column.setOrdinalPosition(ordinalPosition++);
            columns.add(column);
        }
    }

    public static void createTable(String createTableString) {

        ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

        String tableName = createTableTokens.get(2);

        if (tableName.contains("(")) {
            tableName = tableName.substring(0, tableName.indexOf("("));
        }

        table = new Table();
        table.setTableName(tableName);
        table.loadTable();

        parseColumns(createTableString);

        try {
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
                    DavisBaseBinaryFile.getTablePath(DavisBaseBinaryFile.davisbaseTables), "rw");
            Table davisbaseTableMetaData = new Table(DavisBaseBinaryFile.davisbaseTables);

            int pageNo = BPlusTreeImpl.getPageNoForInsert(davisbaseTablesCatalog, davisbaseTableMetaData.getRootPageNo());

            PageUtil page = new PageUtil(davisbaseTablesCatalog, pageNo);

            int rowNo = page.addTableRow(DavisBaseBinaryFile.davisbaseTables,
                    Arrays.asList(new Attribute(DataType.TEXT, tableName),
                            new Attribute(DataType.INT, "0"), new Attribute(DataType.SMALLINT, "0"),
                            new Attribute(DataType.SMALLINT, "0")));
            davisbaseTablesCatalog.close();

            if (rowNo == -1) {
                DavisBaseUtil.printFail("Duplicate table Name");
                return;
            }
            RandomAccessFile tableFile = new RandomAccessFile(DavisBaseBinaryFile.getTablePath(tableName), "rw");
            PageUtil.addNewPage(tableFile, PageType.LEAF, -1, -1);
            tableFile.close();

            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(
                    DavisBaseBinaryFile.getTablePath(DavisBaseBinaryFile.davisbaseColumns), "rw");
            Table davisbaseColumns = new Table(DavisBaseBinaryFile.davisbaseColumns);
            pageNo = BPlusTreeImpl.getPageNoForInsert(davisbaseColumnsCatalog, davisbaseColumns.getRootPageNo());

            PageUtil page1 = new PageUtil(davisbaseColumnsCatalog, pageNo);

            for (Column column : columns) {
                page1.addNewColumn(column);
            }

            davisbaseColumnsCatalog.close();

            DavisBaseUtil.printSuccess("Table created");
        } catch (Exception e) {
            System.out.println("Exception on creating Table");
            System.out.println(e);
        }
    }

}
