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
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.HashSet;

public class HashJoin extends manipulateData {

    public static List<String> performHashJoinHashingSmallTable(List<String> table1Data, List<String> table2Data) {    
        List<String> joinedDataSmallTableHash = new ArrayList<>();
    
        Map<String, List<String>> hashTable;
        List<String> largerTable;
    
        if (table1Data.size() <= table2Data.size()) {
            hashTable = buildHashTable(table1Data);
            //smallerTable = table1Data;
            largerTable = table2Data;
        } else {
            hashTable = buildHashTable(table2Data);
            //smallerTable = table2Data;
            largerTable = table1Data;
        }
    
        for (String row : largerTable) {
            String value = getValue(row);
            if (hashTable.containsKey(value)) {
                List<String> matchingRows = hashTable.get(value);
                for (String matchingRow : matchingRows) {
                    if (isTimestampWithinLimit(matchingRow, row, 10)) {
                        String joinedRow = matchingRow + ", " + row;
                        joinedDataSmallTableHash.add(joinedRow);
                    }
                }
            }
        }
    
        return joinedDataSmallTableHash;
    }

    public static List<String> performHashJoinHashingLargeTable(List<String> table1Data, List<String> table2Data) {
        List<String> joinedDataLargeTableHash = new ArrayList<>();
    
        Map<String, List<String>> hashTable;
        List<String> smallerTable;
    
        if (table1Data.size() <= table2Data.size()) {
            hashTable = buildHashTable(table2Data);
            smallerTable = table1Data;
            //largerTable = table2Data;
        } else {
            hashTable = buildHashTable(table1Data);
            smallerTable = table2Data;
            //largerTable = table1Data;
        }
    
        for (String row : smallerTable) {
            String value = getValue(row);
            if (hashTable.containsKey(value)) {
                List<String> matchingRows = hashTable.get(value);
                for (String matchingRow : matchingRows) {
                    if (isTimestampWithinLimit(matchingRow, row, 10)) { 
                        String joinedRow = matchingRow + ", " + row; 
                        joinedDataLargeTableHash.add(joinedRow);
                    }
                }
            }
        }
    
        return joinedDataLargeTableHash;
    }

    public static List<String> performHashJoinWithTimestampChecksFirstHashingSmall(List<String> table1Data, List<String> table2Data) {
        List<String> joinedDataSmallTableHashTimestampFirst = new ArrayList<>();
    
        List<String> largerTable;
        Map<String, List<String>> hashTable;
    
        List<String> ArrayListTable1 = new ArrayList<>();
        List<String> ArrayListTable2 = new ArrayList<>();
    
        for (String row1 : table1Data) {
            for (String row2 : table2Data) {
                if (isTimestampWithinLimit(row1, row2, 10)) {
                    if (!ArrayListTable1.contains(row1)) {
                        ArrayListTable1.add(row1);
                    }
                    if (!ArrayListTable2.contains(row2)) {
                        ArrayListTable2.add(row2);
                    }
                }
            }
        }

        if (ArrayListTable1.size() <= ArrayListTable2.size()) {
            hashTable = buildHashTable(ArrayListTable1);
            //smallerTable = ArrayListTable1;
            largerTable = ArrayListTable2;
        } else {
            hashTable = buildHashTable(ArrayListTable2);
            //smallerTable = ArrayListTable2;
            largerTable = ArrayListTable1;
        }
        
        for (String row : largerTable) {
            String value = getValue(row);
            if (hashTable.containsKey(value)) {
                List<String> matchingRows = hashTable.get(value);
                for (String matchingRow : matchingRows) {               
                    String joinedRow = matchingRow + ", " + row;
                    joinedDataSmallTableHashTimestampFirst.add(joinedRow);
                }
            }
        }
    
        return joinedDataSmallTableHashTimestampFirst;
    }

    public static List<String> performHashJoinWithTimestampChecksFirstHashingLarge(List<String> table1Data, List<String> table2Data) {
        List<String> joinedDataLargeTableHashTimestampFirst = new ArrayList<>();
    
        List<String> smallerTable;
        Map<String, List<String>> hashTable;
    
        List<String> ArrayListTable1 = new ArrayList<>();
        List<String> ArrayListTable2 = new ArrayList<>();
    
        for (String row1 : table1Data) {
            for (String row2 : table2Data) {
                if (isTimestampWithinLimit(row1, row2, 10)) {
                    if (!ArrayListTable1.contains(row1)) {
                        ArrayListTable1.add(row1);
                    }
                    if (!ArrayListTable2.contains(row2)) {
                        ArrayListTable2.add(row2);
                    }
                }
            }
        }
    
        if (ArrayListTable1.size() <= ArrayListTable2.size()) {
            hashTable = buildHashTable(ArrayListTable2);
            smallerTable = ArrayListTable1;
            //largerTable = ArrayListTable2;
        } else {
            hashTable = buildHashTable(ArrayListTable1);
            smallerTable = ArrayListTable2;
            //largerTable = ArrayListTable1;
        }
        
        for (String row : smallerTable) {
            String value = getValue(row);
            if (hashTable.containsKey(value)) {
                List<String> matchingRows = hashTable.get(value);
                for (String matchingRow : matchingRows) {
                    String joinedRow = matchingRow + ", " + row; 
                    joinedDataLargeTableHashTimestampFirst.add(joinedRow);                
                }
            }
        }
    
        return joinedDataLargeTableHashTimestampFirst;
    } 

    private static Map<String, List<String>> buildHashTable(List<String> tableData) {
        Map<String, List<String>> hashTable = new HashMap<>();
        for (String row : tableData) {
            String value = getValue(row);
            if (!hashTable.containsKey(value)) {
                hashTable.put(value, new ArrayList<>());
            }
            hashTable.get(value).add(row);
        }
        return hashTable;
    }

    private static boolean isTimestampWithinLimit(String row1, String row2, int limitInSeconds) {
        String timestamp1 = getTimestamp(row1);
        String timestamp2 = getTimestamp(row2);
    
        long difference = Math.abs(parseTimestamp(timestamp1) - parseTimestamp(timestamp2));
    
        return difference <= (limitInSeconds * 1000);
    }
    
    private static long parseTimestamp(String timestamp) {
        try {
            return Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return 0;
        }
    }    
    
    private static String getTimestamp(String row) {
        String[] parts = row.split(",");
        String timestampPart = parts[2].trim();
        return timestampPart.substring(timestampPart.indexOf(":") + 2);
    }
}