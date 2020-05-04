package com.davisbase.lite;

import com.davisbase.lite.metadata.*;
import com.davisbase.lite.utils.BPlusTreeImpl;
import com.davisbase.lite.utils.ConditionParser;
import com.davisbase.lite.utils.DataTypeUtil;
import com.davisbase.lite.utils.PageUtil;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

import static java.lang.System.out;


/**
 * @author Chris Irwin Davis
 * @version 1.0
 */
public class DavisBaseBinaryFile {

    /* This static variable controls page size. */
    public static int pageSizePower = 9;
    /* This strategy insures that the page size is always a power of 2. */
    public static int pageSize = (int) Math.pow(2, pageSizePower);

    public static String dataDir = "data";
    public static String davisbaseTables = "davisbase_tables";
    public static String davisbaseColumns = "davisbase_columns";

    public static boolean isDataStoreInitialized = false;

    private RandomAccessFile file;

    public DavisBaseBinaryFile(RandomAccessFile tableFile) {
        this.file = tableFile;
    }

    public static String getTablePath(String table) {
        return dataDir + File.separator + table + ".tbl";
    }

    static boolean isIsDataStoreInitialized() {
        isDataStoreInitialized = new File(getTablePath(davisbaseTables)).exists()
                && new File(getTablePath(davisbaseColumns)).exists();
        return isDataStoreInitialized;
    }

    /**
     * This static method creates the DavisBase data storage container
     * and then initializes two .tbl files to implement the two
     * system tables, davisbase_tables and davisbase_columns
     * <p>
     * WARNING! Calling this method will destroy the system database
     * catalog files if they already exist.
     */
    static void initializeDataStore() {

        if (isIsDataStoreInitialized()) {
            return;
        }

        /** Create data directory at the current OS location to hold */
        try {
            File dataDir = new File("data");
            dataDir.mkdir();
            String[] oldTableFiles;
            oldTableFiles = dataDir.list();
            for (int i = 0; i < oldTableFiles.length; i++) {
                File anOldFile = new File(dataDir, oldTableFiles[i]);
                anOldFile.delete();
            }
        } catch (SecurityException se) {
            out.println("Unable to create data container directory");
            out.println(se);
        }

        /** Create davisbase_tables system catalog */
        try {
            RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(getTablePath(davisbaseTables), "rw");
            PageUtil.addNewPage(davisbaseTablesCatalog, PageType.LEAF, -1, -1);
            PageUtil page = new PageUtil(davisbaseTablesCatalog, 0);
            addDefaultTableData(page);
            davisbaseTablesCatalog.close();
        } catch (Exception e) {
            out.println("Unable to create the database_tables file");
            out.println(e);
        }

        /** Create davisbase_columns systems catalog */
        try {
            RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile(getTablePath(davisbaseColumns), "rw");
            PageUtil.addNewPage(davisbaseColumnsCatalog, PageType.LEAF, -1, -1);
            PageUtil page = new PageUtil(davisbaseColumnsCatalog, 0);
            addDefaultColumnData(page);
            davisbaseColumnsCatalog.close();
        } catch (Exception e) {
            out.println("Unable to create the database_columns file");
            out.println(e);
        }

        postInit();
    }

    private static void addDefaultTableData(PageUtil page) throws Exception {
        page.addTableRow(davisbaseTables, Arrays.asList(new Attribute(DataType.TEXT, DavisBaseBinaryFile.davisbaseTables),
                new Attribute(DataType.INT, "2"),
                new Attribute(DataType.SMALLINT, "0"),
                new Attribute(DataType.SMALLINT, "0")));

        page.addTableRow(davisbaseTables, Arrays.asList(new Attribute(DataType.TEXT, DavisBaseBinaryFile.davisbaseColumns),
                new Attribute(DataType.INT, "11"),
                new Attribute(DataType.SMALLINT, "0"),
                new Attribute(DataType.SMALLINT, "2")));
    }

    private static void addDefaultColumnData(PageUtil page) throws IOException {
        short ordinal_position = 1;

        page.addNewColumn(new Column(davisbaseTables, DataType.TEXT, "table_name", true, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseTables, DataType.INT, "record_count", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseTables, DataType.SMALLINT, "avg_length", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseTables, DataType.SMALLINT, "root_page", false, false, ordinal_position));


        ordinal_position = 1;

        page.addNewColumn(new Column(davisbaseColumns, DataType.TEXT, "table_name", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.TEXT, "column_name", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.SMALLINT, "data_type", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.SMALLINT, "ordinal_position", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.TEXT, "is_nullable", false, false, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.SMALLINT, "column_key", false, true, ordinal_position++));
        page.addNewColumn(new Column(davisbaseColumns, DataType.SMALLINT, "is_unique", false, false, ordinal_position));
    }

    /*
       Method for post initialization
     */
    static void postInit() {
        isDataStoreInitialized = true;
    }

    static int getPageSize() {
        return pageSize;
    }

    public boolean rowExists(Table table, String columnName, ConditionParser condition) throws IOException {
        BPlusTreeImpl bPlusOneTree = new BPlusTreeImpl(file, table.getRootPageNo(), table.getTableName());


        for (Integer pageNo : bPlusOneTree.getAllLeaves(condition)) {
            PageUtil page = new PageUtil(file, pageNo);
            for (Row row : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(row.getAttributes().get(condition.columnOrdinal).fieldValueStr))
                        continue;
                }
                return true;
            }
        }
        return false;

    }

    public void updateRows(Table tablemetaData, ConditionParser condition,
                           List<String> columNames, List<String> newVals) throws IOException {
        int count = 0;


        List<Integer> ordinalPostions = tablemetaData.getOrdinalPostions(columNames);


        int k = 0;
        Map<Integer, Attribute> newValueMap = new HashMap<>();

        for (String newValue : newVals) {
            int index = ordinalPostions.get(k);

            try {
                newValueMap.put(index,
                        new Attribute(tablemetaData.getColumns().get(index).getDataType(), newValue));
            } catch (Exception e) {
                System.out.println("Invalid data format for " + tablemetaData.getColumns().get(index) + " values: "
                        + newValue);
                return;
            }

            k++;
        }

        BPlusTreeImpl bPlusOneTree = new BPlusTreeImpl(file, tablemetaData.getRootPageNo(), tablemetaData.getTableName());

        for (Integer pageNo : bPlusOneTree.getAllLeaves(condition)) {
            PageUtil page = new PageUtil(file, pageNo);
            for (Row record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValueStr))
                        continue;
                }
                count++;
                for (int i : newValueMap.keySet()) {
                    if ((record.getAttributes().get(i).dataType == DataType.TEXT
                            && record.getAttributes().get(i).fieldValueStr.length() == newValueMap.get(i).fieldValueStr.length())
                            || (record.getAttributes().get(i).dataType != DataType.NULL && record.getAttributes().get(i).dataType != DataType.TEXT)
                    ) {
                        page.updateRow(record, i, newValueMap.get(i).fieldValueByte);
                    }
                }
            }
        }

        if (!tablemetaData.getTableName().equals(davisbaseTables) && !tablemetaData.getTableName().equals(davisbaseColumns))
            System.out.println("* " + count + " record(s) updated.");

    }

    public void selectRecords(Table table, List<String> columnNames, ConditionParser condition) throws IOException {
        List<Integer> ordinalPostions = table.getOrdinalPostions(columnNames);

        System.out.println();

        List<Integer> printPosition = new ArrayList<>();

        int columnLength = 0;
        printPosition.add(columnLength);
        int totalTableLength = 0;

        totalTableLength = printColumnHeaders(table, ordinalPostions, printPosition, totalTableLength);
        System.out.println();
        System.out.println(DavisBasePrompt.line("-", totalTableLength));

        BPlusTreeImpl bPlusOneTree = new BPlusTreeImpl(file, table.getRootPageNo(), table.getTableName());

        printColumnValues(condition, ordinalPostions, printPosition, bPlusOneTree);

        System.out.println();

    }

    private void printColumnValues(ConditionParser condition, List<Integer> ordinalPostions, List<Integer> printPosition, BPlusTreeImpl bPlusOneTree) throws IOException {
        String currentValue;
        for (Integer pageNo : bPlusOneTree.getAllLeaves(condition)) {
            PageUtil page = new PageUtil(file, pageNo);
            for (Row record : page.getPageRecords()) {
                if (condition != null) {
                    if (!condition.checkCondition(record.getAttributes().get(condition.columnOrdinal).fieldValueStr))
                        continue;
                }
                int columnCount = 0;
                for (int i : ordinalPostions) {
                    currentValue = record.getAttributes().get(i).fieldValueStr;
                    System.out.print(currentValue);
                    System.out.print(DavisBasePrompt.line(" ", printPosition.get(++columnCount) - currentValue.length() -2) + "| ");
                }
                System.out.println();
            }
        }
    }

    private int printColumnHeaders(Table table, List<Integer> ordinalPostions, List<Integer> printPosition, int totalTableLength) {
        int columnLength;
        for (int i : ordinalPostions) {
            String columnName = table.getColumns().get(i).getColumnName();
            columnLength = Math.max(columnName.length()
                    , DataTypeUtil.getPrintOffset(table.getColumns().get(i).getDataType())) + 3;
            printPosition.add(columnLength);
            System.out.print(columnName);
            System.out.print(DavisBasePrompt.line(" ", columnLength - columnName.length() -2) + "| ");
            totalTableLength += columnLength;
        }
        return totalTableLength;
    }
}
