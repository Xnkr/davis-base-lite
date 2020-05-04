package com.davisbase.lite.utils;

import com.davisbase.lite.metadata.DataType;
import com.davisbase.lite.metadata.Operator;

public class ConditionParser {

    String columnName;
    private Operator operator;
    String comparisonValue;
    boolean negation;
    public int columnOrdinal;
    public DataType dataType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public String getComparisonValue() {
        return comparisonValue;
    }

    public void setComparisonValue(String comparisonValue) {
        this.comparisonValue = comparisonValue;
    }

    public boolean isNegation() {
        return negation;
    }

    public int getColumnOrdinal() {
        return columnOrdinal;
    }

    public void setColumnOrdinal(int columnOrdinal) {
        this.columnOrdinal = columnOrdinal;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public ConditionParser(DataType dataType) {
        this.dataType = dataType;
    }

    public static String[] supportedOperators = {"<=", ">=", "<>", ">", "<", "="};


    public static Operator getOperator(String strOperator) {
        switch (strOperator) {
            case ">":
                return Operator.GREATERTHAN;
            case "<":
                return Operator.LESSTHAN;
            case "=":
                return Operator.EQUALTO;
            case ">=":
                return Operator.GREATERTHANOREQUAL;
            case "<=":
                return Operator.LESSTHANOREQUAL;
            case "<>":
                return Operator.NOTEQUAL;
            default:
                System.out.println("Invalid operator \"" + strOperator + "\"");
                return Operator.INVALID;
        }
    }

    public static int compare(String value1, String value2, DataType dataType) {
        if (dataType == DataType.TEXT)
            return value1.toLowerCase().compareTo(value2);
        else if (dataType == DataType.NULL) {
            if (value1.equals(value2))
                return 0;
            else if (value1.toLowerCase().equals("null"))
                return 1;
            else
                return -1;
        } else {
            return Long.valueOf(Long.parseLong(value1) - Long.parseLong(value2)).intValue();
        }
    }

    private boolean doOperationOnDifference(Operator operation, int difference) {
        switch (operation) {
            case LESSTHANOREQUAL:
                return difference <= 0;
            case GREATERTHANOREQUAL:
                return difference >= 0;
            case NOTEQUAL:
                return difference != 0;
            case LESSTHAN:
                return difference < 0;
            case GREATERTHAN:
                return difference > 0;
            case EQUALTO:
                return difference == 0;
            default:
                return false;
        }
    }

    private boolean doStringCompare(String currentValue, Operator operation) {
        return doOperationOnDifference(operation, currentValue.toLowerCase().compareTo(comparisonValue));
    }


    public boolean checkCondition(String currentValue) {
        Operator operation = getOperation();

        if (currentValue.toLowerCase().equals("null")
                || comparisonValue.toLowerCase().equals("null"))
            return doOperationOnDifference(operation, compare(currentValue, comparisonValue, DataType.NULL));

        if (dataType == DataType.TEXT || dataType == DataType.NULL)
            return doStringCompare(currentValue, operation);
        else {
            switch (operation) {
                case LESSTHANOREQUAL:
                    return Double.parseDouble(currentValue) <= Double.parseDouble(comparisonValue);
                case GREATERTHANOREQUAL:
                    return Double.parseDouble(currentValue) >= Double.parseDouble(comparisonValue);
                case NOTEQUAL:
                    return Double.parseDouble(currentValue) != Double.parseDouble(comparisonValue);
                case LESSTHAN:
                    return Double.parseDouble(currentValue) < Double.parseDouble(comparisonValue);
                case GREATERTHAN:
                    return Double.parseDouble(currentValue) > Double.parseDouble(comparisonValue);
                case EQUALTO:
                    return Double.parseDouble(currentValue) == Double.parseDouble(comparisonValue);

                default:
                    return false;

            }
        }

    }

    public void setConditionValue(String conditionValue) {
        this.comparisonValue = conditionValue;
        this.comparisonValue = comparisonValue.replace("'", "");
        this.comparisonValue = comparisonValue.replace("\"", "");

    }

    public void setColumName(String columnName) {
        this.columnName = columnName;
    }

    public void setOperator(String operator) {
        this.operator = getOperator(operator);
    }

    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    public Operator getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }


    private Operator negateOperator() {
        switch (this.operator) {
            case LESSTHANOREQUAL:
                return Operator.GREATERTHAN;
            case GREATERTHANOREQUAL:
                return Operator.LESSTHAN;
            case NOTEQUAL:
                return Operator.EQUALTO;
            case LESSTHAN:
                return Operator.GREATERTHANOREQUAL;
            case GREATERTHAN:
                return Operator.LESSTHANOREQUAL;
            case EQUALTO:
                return Operator.NOTEQUAL;
            default:
                System.out.println("Invalid operator \"" + this.operator + "\"");
                return Operator.INVALID;
        }
    }


}
