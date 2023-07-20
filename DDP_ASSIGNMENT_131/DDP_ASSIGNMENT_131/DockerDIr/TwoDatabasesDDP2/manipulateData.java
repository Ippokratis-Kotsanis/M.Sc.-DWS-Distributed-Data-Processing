import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class manipulateData {

    public static void main(String[] args) {
        Connection connection1 = null;
        try {
            connection1 = DriverManager.getConnection("jdbc:sqlite:/volumes/database1/database1.db");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        Connection connection2 = null;
        try {
            connection2 = DriverManager.getConnection("jdbc:sqlite:/volumes/database2/database2.db");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        //Print the data of the 2 databases    
        Map<String, List<String>> tableData = new HashMap<>();

        tableData.put("Table1 - Volume1", getData(connection1, "table1"));
        tableData.put("Table2 - Volume1", getData(connection1, "table2"));

        tableData.put("Table1 - Volume2", getData(connection2, "table1"));
        tableData.put("Table2 - Volume2", getData(connection2, "table2"));

        for (Map.Entry<String, List<String>> entry : tableData.entrySet()) {
            String tableName = entry.getKey();
            List<String> data = entry.getValue();
            System.out.println("Data of " + tableName + ":");
            for (String rowData : data) {
                System.out.println(rowData);
            }
            System.out.println();
        }

        List<String> table1DataVolume1 = getData(connection1, "table1");
        List<String> table1DataVolume2 = getData(connection2, "table2");


        // ------------------------------------------------------------------------------------------ //
        // ---------------------------------- HASH JOIN --------------------------------------------- //
        // ------------------------------------------------------------------------------------------ //

        

        // ------------------------------- LAZY IMPLEMENTATION --------------------------------------- //

        // ---- Implementation of hash join - Hashing at first the small table and do checks with the large ----   

        //log the execution time before hash join
        long startTime1 = System.currentTimeMillis();

        List<String> joinedDataSmallTableHash = HashJoin.performHashJoinHashingSmallTable(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime1 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime1 = endTime1 - startTime1;
        System.out.println("Execution Time: " + executionTime1 + " milliseconds");

        System.out.println("Hash Join - Joined Data - Small table Hash - LAZY:");
        printData(joinedDataSmallTableHash);


        // ---- Implementation of hash join - Hashing at first the large table and do checks with the small ----   

        //log the execution time before hash join
        long startTime2 = System.currentTimeMillis();

        List<String> joinedDataLargeTableHash = HashJoin.performHashJoinHashingLargeTable(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime2 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime2 = endTime2 - startTime2;
        System.out.println("Execution Time: " + executionTime2 + " milliseconds");

        System.out.println("Hash Join - Joined Data - Large table Hash - LAZY:");
        printData(joinedDataLargeTableHash);




        // ------------------------------- EAGER IMPLEMENTATION --------------------------------------- //



        // ---- Implementation of hash join - Doing firstly the Timespamp checks and Hashing at first the small table and do checks with the large ----   

        //log the execution time before hash join
        long startTime3 = System.currentTimeMillis();

        List<String> joinedDataSmallTableHashTimestampFirst = HashJoin.performHashJoinWithTimestampChecksFirstHashingSmall(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime3 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime3 = endTime3 - startTime3;
        System.out.println("Execution Time: " + executionTime3 + " milliseconds");

        System.out.println("Hash Join - Joined Data - Small table Hash - First do Timestamp checks - EAGER:");
        printData(joinedDataSmallTableHashTimestampFirst);


        // ---- Implementation of hash join - Doing firstly the Timespamp chencks and Hashing at first the large table and do checks with the small ----   

        //log the execution time before hash join
        long startTime4 = System.currentTimeMillis();

        List<String> joinedDataLargeTableHashTimestampFirst = HashJoin.performHashJoinWithTimestampChecksFirstHashingLarge(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime4 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime4 = endTime4 - startTime4;
        System.out.println("Execution Time: " + executionTime4 + " milliseconds");

        System.out.println("Hash Join - Joined Data - Large table Hash - First do Timestamp checks - EAGER:");
        printData(joinedDataLargeTableHashTimestampFirst);
       

        // ------------------------------------------------------------------------------------------ //
        // ------------------------------ PIPELINE HASH JOIN ---------------------------------------- //
        // ------------------------------------------------------------------------------------------ //

        

        // ------------------------------- LAZY IMPLEMENTATION --------------------------------------- //


        // ---- Implementation of pipeline hash join ------   

        //log the execution time before pipeline hash join
        long startTime5 = System.currentTimeMillis();

        List<String> joinedDataLazy = pipelineHashJoin.performPipelineHashJoinLazy(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime5 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime5 = endTime5 - startTime5;
        System.out.println("Execution Time: " + executionTime5 + " milliseconds");

        System.out.println("Pipeline Hash Join - Joined Data - LAZY:");
        printData(joinedDataLazy);


        // ------------------------------- EAGER IMPLEMENTATION --------------------------------------- //


        // ---- Implementation of pipeline hash join ------   

        //log the execution time before pipeline hash join
        long startTime6 = System.currentTimeMillis();

        List<String> joinedDataEager = pipelineHashJoin.performPipelineHashJoinEager(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime6 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime6 = endTime6 - startTime6;
        System.out.println("Execution Time: " + executionTime6 + " milliseconds");

        System.out.println("Pipeline Hash Join - Joined Data - EAGER:");
        printData(joinedDataEager);


        // ------------------------------------------------------------------------------------------ //
        // ---------------------------------------- SEMI JOIN --------------------------------------- //
        // ------------------------------------------------------------------------------------------ //


        // ------------------------------- LAZY IMPLEMENTATION --------------------------------------- //

        // ------------------ Implementation of semi join - Table1 semi-join Table2 ----------------- //  

        //log the execution time before pipeline hash join
        long startTime7 = System.currentTimeMillis();

        List<String> semiJoinedTable1Data = semiJoin.performTable1DataSemiJoinTable2Data(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime7 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime7 = endTime7 - startTime7;
        System.out.println("Execution Time: " + executionTime7 + " milliseconds");

        System.out.println("Semi Join - Semi Joined Data - Table1 - LAZY:");
        printData(semiJoinedTable1Data);


        // ------------------------------- EAGER IMPLEMENTATION --------------------------------------- //

        // ------------------ Implementation of semi join - Table1 semi-join Table2 ----------------- //  

        //log the execution time before pipeline hash join
        long startTime8 = System.currentTimeMillis();

        List<String> semiJoinedTable1DataTimestampFirst = semiJoin.performTable1DataSemiJoinTable2DataTimestampFirst(table1DataVolume1, table1DataVolume2);

        // Stop logging the execution time
        long endTime8 = System.currentTimeMillis();

        // Calculate and print the execution time
        long executionTime8 = endTime8 - startTime8;
        System.out.println("Execution Time: " + executionTime8 + " milliseconds");

        System.out.println("Semi Join - Semi Joined Data - Table1 - Check Timestamp first - EAGER:");
        printData(semiJoinedTable1DataTimestampFirst);


        try {
            connection1.close();
            connection2.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private static List<String> getData(Connection connection, String tableName) {
        List<String> data = new ArrayList<>();
        try {
            Statement statement = connection.createStatement();
            String query = "SELECT * FROM " + tableName;
            ResultSet resultSet = statement.executeQuery(query);

            while (resultSet.next()) {
                String recordId = resultSet.getString("record_id");
                String value = resultSet.getString("value");
                String timestamp = resultSet.getString("timestamp");
                String rowData = "Record ID: " + recordId + ", Value: " + value + ", Timestamp: " + timestamp;
                data.add(rowData);
            }

            resultSet.close();
            statement.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        return data;
    }

    private static void printData(List<String> data) {
        for (String rowData : data) {
            System.out.println(rowData);
        }
        System.out.println();
    }

    public static String getValue(String row) {
        String[] parts = row.split(",");
        String valuePart = parts[1];
        return valuePart.substring(valuePart.indexOf(":") + 2);
    }
}