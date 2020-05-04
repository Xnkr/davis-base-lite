package com.davisbase.lite;

import com.davisbase.lite.actions.CreateTable;
import com.davisbase.lite.actions.InsertIntoTable;
import com.davisbase.lite.actions.SelectFromTable;
import com.davisbase.lite.utils.DavisBaseUtil;

import java.util.*;

import static java.lang.System.out;

/**
 *  @author Chris Irwin Davis
 *  @version 1.0
 *  <b>
 *  <p>This is an example of how to create an interactive prompt</p>
 *  <p>There is also some guidance to get started with read/write of
 *     binary data files using RandomAccessFile class</p>
 *  </b>
 *
 */
public class DavisBasePrompt {

    /* This can be changed to whatever you like */
    static String prompt = "davisql> ";
    static String version = "v1.0b(example)";
    static String copyright = "©2016 Chris Irwin Davis";
    static boolean isExit = false;
    /*
     * Page size for alll files is 512 bytes by default.
     * You may choose to make it user modifiable
     */
    static long pageSize = 512;

    /*
     *  The Scanner class is used to collect user commands from the prompt
     *  There are many ways to do this. This is just one.
     *
     *  Each time the semicolon (;) delimiter is entered, the userCommand
     *  String is re-populated.
     */
    static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    /** ***********************************************************************
     *  Main method
     */
    public static void main(String[] args) {

        /* Display the welcome screen */
        splashScreen();

        DavisBaseBinaryFile.initializeDataStore();

        /* Variable to collect user input from the prompt */
        String userCommand = "";

        while(!isExit) {
            out.print(prompt);
            /* toLowerCase() renders command case insensitive */
            userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
            // userCommand = userCommand.replace("\n", "").replace("\r", "");
            parseUserCommand(userCommand);
        }
        out.println("Exiting...");


    }

    /** ***********************************************************************
     *  Static method definitions
     */

    /**
     *  Display the splash screen
     */
    public static void splashScreen() {
        System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        System.out.println("DavisBaseLite Version " + getVersion());
        System.out.println(getCopyright());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(line("-",80));
    }

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s,int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static void printCmd(String s) {
        System.out.println("\n\t" + s + "\n");
    }
    public static void printDef(String s) {
        System.out.println("\t\t" + s);
    }

    /**
     *  Help: Display supported commands
     */
    public static void help() {
        out.println(line("*",80));
        out.println("SUPPORTED COMMANDS\n");
        out.println("All commands below are case insensitive\n");
        out.println("SHOW TABLES;");
        out.println("\tDisplay the names of all tables.\n");
        out.println("CREATE TABLE <table_name> (<column_name> <data_type> <not_null> <unique>);");
        out.println("\t Creates tables with given columns\n");
        out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
        out.println("\tDisplay table records whose optional <condition>");
        out.println("\tis <column_name> = <value>.\n");
        out.println("DROP TABLE <table_name>;");
        out.println("\tRemove table data (i.e. all records) and its schema.\n");
        out.println("INSERT INTO <table_name> (<column_name>) VALUES (<value>);");
        out.println("\tModify records data whose optional <condition> is\n");
        out.println("VERSION;");
        out.println("\tDisplay the program version.\n");
        out.println("HELP;");
        out.println("\tDisplay this help information.\n");
        out.println("EXIT;");
        out.println("\tExit the program.\n");
        out.println(line("*",80));
    }

    /** return the DavisBase version */
    public static String getVersion() {
        return version;
    }

    public static String getCopyright() {
        return copyright;
    }

    public static void displayVersion() {
        System.out.println("DavisBaseLite Version " + getVersion());
        System.out.println(getCopyright());
    }

    public static void parseUserCommand (String userCommand) {

        /* commandTokens is an array of Strings that contains one token per array element
         * The first token can be used to determine the type of command
         * The other tokens can be used to pass relevant parameters to each command-specific
         * method inside each case statement */
        // String[] commandTokens = userCommand.split(" ");
        ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));


        /*
         *  This switch handles a very small list of hardcoded commands of known syntax.
         *  You will want to rewrite this method to interpret more complex commands.
         */
        switch (commandTokens.get(0)) {
            case "select":
                System.out.println("CASE: SELECT");
                parseQuery(userCommand);
                break;
            case "show":
                if (commandTokens.get(1).equals("tables"))
                    showTables();
                break;
            case "create":
                System.out.println("CASE: CREATE");
                parseCreateTable(userCommand);
                break;
            case "insert":
                out.println("CASE: INSERT");
                parseInsertIntoTable(userCommand);
                break;
            case "help":
                help();
                break;
            case "version":
                displayVersion();
                break;
            case "exit":
                isExit = true;
                break;
            case "quit":
                isExit = true;
            case "devtest":
                int testRecords = 50;
                if (commandTokens.size() > 1)
                    testRecords = Integer.parseInt(commandTokens.get(1));
                runSystemTest(testRecords);
                break;
            default:
                System.out.println("I didn't understand the command: \"" + userCommand + "\"");
                break;
        }
    }

    /**
     *  Method for show tables
     */
    private static void showTables() {
        parseQuery("select * from davisbase_tables");
    }

    /**
     *  Method for running all queries
     * @param testRecords
     */
    private static void runSystemTest(int testRecords) {
        DavisBaseUtil.printSuccess("Running System test");
        String testQuery = "CREATE TABLE dummy (id INT PRIMARY KEY, ch text, fl float not null, dt datetime)";
        out.println(testQuery);
        parseCreateTable(testQuery);
        testQuery = "INSERT INTO dummy (id, ch, fl, dt) VALUES (%d, '%s', %f, '%s')";
        for (int id = 0; id < testRecords; id++) {
            String ch = DavisBaseUtil.getRandomString();
            float fl = (id + 1) * 113.0f;
            String dt = DavisBaseUtil.getRandomDateTime();
            String insertQuery = String.format(testQuery, id, ch, fl, dt);
            out.println(insertQuery);
            parseInsertIntoTable(insertQuery);
        }
        testQuery = "SELECT * FROM dummy WHERE id < 5";
        out.println(testQuery);
        parseQuery(testQuery);
        testQuery = "SHOW TABLES";
        out.println(testQuery);
        showTables();
    }

    /**
     *  Method for insert queries
     *  @param userCommand is a String of the user input
     */
    public static void parseInsertIntoTable(String userCommand) {
        InsertIntoTable.insertInto(userCommand.toLowerCase());
    }


    /**
     *  Method for executing queries
     *  @param queryString is a String of the user input
     */
    public static void parseQuery(String queryString) {
        SelectFromTable.selectFromTable(queryString.toLowerCase());
    }

    /**
     *  Method for creating new tables
     *  @param createTableString is a String of the user input
     */
    public static void parseCreateTable(String createTableString) {
        CreateTable.createTable(createTableString.toLowerCase());
    }
}