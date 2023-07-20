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

public class pipelineHashJoin extends manipulateData {

    public static List<String> performPipelineHashJoinLazy(List<String> table1Data, List<String> table2Data) {
        List<String> joinedDataLazy = new ArrayList<>(); 
        Map<String, List<String>> hashTable1 = new HashMap<>();
        Map<String, List<String>> hashTable2 = new HashMap<>();

        List<String> mergeTable = new ArrayList<>();
        mergeTable.addAll(table1Data);
        mergeTable.addAll(table2Data);
    
        long seed = 12345; 
        Collections.shuffle(mergeTable, new Random(seed)); 
    
        for (String row : mergeTable) {
            String value = getValue(row);
    
            if (table1Data.contains(row) ) {
                if (!hashTable1.containsKey(value)) {
                    hashTable1.put(value, new ArrayList<>());
                }
                hashTable1.get(value).add(row);
    
                List<String> matchingRows2 = hashTable2.get(value);
                if (matchingRows2 != null) {
                    for (String matchingRow2 : matchingRows2) {
                        if (isTimestampWithinLimit(row, matchingRow2, 10)) {
                            String joinedRow = row + ", " + matchingRow2;
                            joinedDataLazy.add(joinedRow);
                        }
                    }
                }
            } else if (table2Data.contains(row)) {
                if (!hashTable2.containsKey(value)) {
                    hashTable2.put(value, new ArrayList<>());
                }
                hashTable2.get(value).add(row);
    
                List<String> matchingRows1 = hashTable1.get(value);
                if (matchingRows1 != null) {
                    for (String matchingRow1 : matchingRows1) {
                        if (isTimestampWithinLimit(matchingRow1, row, 10)) {
                            String joinedRow = matchingRow1 + ", " + row;
                            joinedDataLazy.add(joinedRow);
                        }
                    }
                }
            }
        }
    
        return joinedDataLazy;
    }

    public static List<String> performPipelineHashJoinEager(List<String> table1Data, List<String> table2Data) {
        Set<String> joinedDataEager = new HashSet<>(); // Set to avoid duplicates, if we use List we have many iterations, so the eager approach is not the optimum solution.
        //List<String> joinedDataEager = new ArrayList<>();
        
        Map<String, List<String>> hashTable1 = new HashMap<>();
        Map<String, List<String>> hashTable2 = new HashMap<>();
    
        List<String> mergeTable = new ArrayList<>();
        mergeTable.addAll(table1Data);
        mergeTable.addAll(table2Data);
    
        long seed = 12345; 
        Collections.shuffle(mergeTable, new Random(seed)); 
    
        for (String row : mergeTable) {
            String value = getValue(row);
            if (table1Data.contains(row)) {
                for (String row2 : table2Data) {
                    if (isTimestampWithinLimit(row, row2, 10)) {
                        if (!hashTable1.containsKey(value)) {
                            hashTable1.put(value, new ArrayList<>());
                        }
                        hashTable1.get(value).add(row);
                    }
                }
                List<String> matchingRows1 = hashTable1.get(value);
                List<String> matchingRows2 = hashTable2.get(value);
                if (matchingRows1 != null && matchingRows2 != null) {
                    for (String matchingRow1 : matchingRows1) {
                        for (String matchingRow2 : matchingRows2) {
                            String joinedRow = matchingRow1 + ", " + matchingRow2;
                            joinedDataEager.add(joinedRow);
                        }
                    }
                }
            } else if (table2Data.contains(row)) {
                for (String row1 : table1Data) {
                    if (isTimestampWithinLimit(row, row1, 10)) {
                        if (!hashTable2.containsKey(value)) {
                            hashTable2.put(value, new ArrayList<>());
                        }
                        hashTable2.get(value).add(row);
                    }
                }
                List<String> matchingRows1 = hashTable1.get(value);
                List<String> matchingRows2 = hashTable2.get(value);
                if (matchingRows1 != null && matchingRows2 != null) {
                    for (String matchingRow1 : matchingRows1) {
                        for (String matchingRow2 : matchingRows2) {
                            String joinedRow = matchingRow1 + ", " + matchingRow2;
                            joinedDataEager.add(joinedRow);
                        }
                    }
                }
            }
        }
    
        return new ArrayList<>(joinedDataEager);
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