package com.davisbase.lite.actions;

import com.davisbase.lite.DavisBaseBinaryFile;
import com.davisbase.lite.metadata.DataType;
import com.davisbase.lite.metadata.Table;
import com.davisbase.lite.utils.ConditionParser;
import com.davisbase.lite.utils.DavisBaseUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SelectFromTable {

    static Table table;

    public static void selectFromTable(String selectQueryStr) {

        ArrayList<String> selectFromTokens = new ArrayList<>(Arrays.asList(selectQueryStr.split(" ")));

        String table_name = "";
        List<String> column_names = new ArrayList<>();


        for (int i = 1; i < selectFromTokens.size(); i++) {
            String currentToken = selectFromTokens.get(i);
            if (currentToken.equals("from")) {
                ++i;
                table_name = selectFromTokens.get(i);
                break;
            }
            if (!currentToken.equals("*") && !currentToken.equals(",")) {
                extractColumns(selectFromTokens, column_names, i);
            }
        }

        table = new Table(table_name);
        if (!table.isTableExists()) {
            DavisBaseUtil.printFail("Table does not exist");
            return;
        }

        ConditionParser condition = null;
        if (selectQueryStr.contains("where")) {
            try {
                condition = extractCondition(table, selectQueryStr);
            } catch (Exception e) {
                DavisBaseUtil.printFail(e.getMessage());
                return;
            }
        }

        if (column_names.size() == 0) {
            column_names = table.getColumnNames();
        }
        try {

            RandomAccessFile tableFile = new RandomAccessFile(DavisBaseBinaryFile.getTablePath(table_name), "r");
            DavisBaseBinaryFile tableBinaryFile = new DavisBaseBinaryFile(tableFile);
            tableBinaryFile.selectRecords(table, column_names, condition);
            tableFile.close();
        } catch (IOException exception) {
            System.out.println("Exception selecting columns from table");
        }
    }

    private static void extractColumns(ArrayList<String> selectFromTokens, List<String> column_names, int i) {
        if (selectFromTokens.get(i).contains(",")) {
            ArrayList<String> colList = new ArrayList<>(
                    Arrays.asList(selectFromTokens.get(i).split(",")));
            for (String col : colList) {
                column_names.add(col.trim());
            }
        } else
            column_names.add(selectFromTokens.get(i));
    }

    private static ConditionParser extractCondition(Table table, String query) throws Exception {

        ConditionParser condition = new ConditionParser(DataType.TEXT);
        String whereClause = query.substring(query.indexOf("where") + 6);
        ArrayList<String> whereClauseTokens = new ArrayList<>(Arrays.asList(whereClause.split(" ")));

        if (whereClauseTokens.get(0).equalsIgnoreCase("not")) {
            condition.setNegation(true);
        }


        for (int i = 0; i < ConditionParser.supportedOperators.length; i++) {
            if (whereClause.contains(ConditionParser.supportedOperators[i])) {
                whereClauseTokens = new ArrayList<>(
                        Arrays.asList(whereClause.split(ConditionParser.supportedOperators[i])));

                condition.setOperator(ConditionParser.supportedOperators[i]);
                condition.setConditionValue(whereClauseTokens.get(1).trim());
                condition.setColumName(whereClauseTokens.get(0).trim());
                break;

            }
        }


        if (table.isTableExists()
                && table.getColumnNames().contains(condition.getColumnName())) {
            condition.columnOrdinal = table.getColumnNames().indexOf(condition.getColumnName());
            condition.dataType = table.getColumns().get(condition.columnOrdinal).getDataType();
        } else {
            throw new Exception(
                    "Invalid Table/Column : " + table.getTableName() + " . " + condition.getColumnName());
        }
        return condition;

    }

}
