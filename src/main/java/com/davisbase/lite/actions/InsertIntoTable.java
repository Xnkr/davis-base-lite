package com.davisbase.lite.actions;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.metadata.Attribute;
import com.davisbase.lite.metadata.Column;
import com.davisbase.lite.metadata.DataType;
import com.davisbase.lite.metadata.Table;
import com.davisbase.lite.utils.BPlusTreeImpl;
import com.davisbase.lite.utils.PageUtil;
import com.davisbase.lite.utils.DavisBaseUtil;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertIntoTable {

    static Table table;

    public static void insertInto(String insertIntoString) {

        List<String> insertTokens = new ArrayList<>(Arrays.asList(insertIntoString.split(" ")));

        try {
            String tableName = insertTokens.get(2);

            if (tableName.contains("(")) {
                tableName = tableName.substring(0, tableName.indexOf("("));
            }
            table = new Table(tableName);

            if (!table.isTableExists()) {
                DavisBaseUtil.printFail("Table does not exist.");
                return;
            }

            List<String> rawColumns = parseColumns(insertIntoString);
            if (rawColumns == null) return;

            String rawValuesStr = insertIntoString.substring(insertIntoString.indexOf("values") + 6, insertIntoString.length() - 1);
            String[] rawValueArr = rawValuesStr.substring(rawValuesStr.indexOf("(") + 1).split(",");
            List<String> rawValues = new ArrayList<>(Arrays.asList(rawValueArr));

            List<Attribute> attributeToInsert = new ArrayList<>();

            if (!parseValues(rawColumns, rawValues, attributeToInsert)) return;


            RandomAccessFile tableFile = new RandomAccessFile(DavisBaseBinaryFile.getTablePath(tableName), "rw");
            int pageNo = BPlusTreeImpl.getPageNoForInsert(tableFile, table.getRootPageNo());
            PageUtil page = new PageUtil(tableFile, pageNo);

            int rowNo = page.addTableRow(tableName, attributeToInsert);

            tableFile.close();
            if (rowNo != -1)
                DavisBaseUtil.printSuccess("Row Inserted");
            System.out.println();

        } catch (Exception ex) {
            System.out.println("Exception while inserting record");
            System.out.println(ex);

        }

    }

    private static boolean parseValues(List<String> rawColumns, List<String> rawValues, List<Attribute> attributeToInsert) throws Exception {
        for (Column column : table.getColumns()) {
            int i = 0;
            boolean columnProvided = false;
            for (i = 0; i < rawColumns.size(); i++) {
                if (rawColumns.get(i).trim().equals(column.getColumnName())) {
                    columnProvided = true;
                    try {
                        if (!insertValueInColumn(rawValues, attributeToInsert, column, i)) return true;
                        break;
                    } catch (Exception e) {
                        System.out.println("Invalid data format for " + rawColumns.get(i) + " values: "
                                + rawValues.get(i));
                        return false;
                    }
                }
            }
            if (rawColumns.size() > i) {
                rawColumns.remove(i);
                rawValues.remove(i);
            }

            if (!columnProvided) {
                if (column.isNullable())
                    attributeToInsert.add(new Attribute(DataType.NULL, "NULL"));
                else {
                    System.out.println("Cannot Insert NULL into " + column.isNullable());
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean insertValueInColumn(List<String> rawValues, List<Attribute> attributeToInsert, Column column, int i) throws Exception {
        String value = rawValues.get(i).replace("'", "").replace("\"", "").trim();
        if (rawValues.get(i).trim().equals("null")) {
            if (!column.isNullable()) {
                System.out.println("Cannot Insert NULL into " + column.getColumnName());
                return false;
            }
            column.setDataType(DataType.NULL);
            value = value.toUpperCase();
        }
        Attribute attr = new Attribute(column.getDataType(), value);
        attributeToInsert.add(attr);
        return true;
    }

    private static List<String> parseColumns(String insertIntoString) {
        String rawColumnsStr = insertIntoString.substring(insertIntoString.indexOf("(") + 1, insertIntoString.indexOf(") values"));
        String[] rawColumnsArr = rawColumnsStr.split(",");
        List<String> rawColumns = new ArrayList<>(Arrays.asList(rawColumnsArr));

        for (String rawColumn : rawColumns) {
            if (!table.getColumnNames().contains(rawColumn.trim())) {
                System.out.println("Invalid column : " + rawColumn.trim());
                return null;
            }
        }
        return rawColumns;
    }

}
