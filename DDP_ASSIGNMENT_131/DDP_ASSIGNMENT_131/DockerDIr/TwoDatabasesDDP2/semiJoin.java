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
import java.util.Set;
import java.util.HashSet;

public class semiJoin extends manipulateData {

    public static List<String> performTable1DataSemiJoinTable2Data(List<String> table1Data, List<String> table2Data) {    
        List<String> semiJoinedTable1Data = new ArrayList<>();
    
        List<String> ArrayListTable2 = new ArrayList<>();
        Set<String> uniqueValues = new HashSet<>();
    
        for (String row2 : table2Data) {
            String value2 = getValue(row2);
            if (!uniqueValues.contains(value2)) {
                ArrayListTable2.add(row2);
                uniqueValues.add(value2);
            }
        }
    
        for (String row2 : ArrayListTable2) {
            String value2 = getValue(row2);
            for (String row1 : table1Data){
                String value1 = getValue(row1);
                if (value2.equals(value1)){
                    if (isTimestampWithinLimit(row1, row2, 10)) { 
                        semiJoinedTable1Data.add(row1);
                    }
                }
            }
        }
    
        return semiJoinedTable1Data;
    }

    public static List<String> performTable1DataSemiJoinTable2DataTimestampFirst(List<String> table1Data, List<String> table2Data) {    
        List<String> semiJoinedTable1DataTimestampFirst = new ArrayList<>();
    
        List<String> ArrayListTable2 = new ArrayList<>();
        Set<String> uniqueValues = new HashSet<>();
        List<String> table1DataTimestampChecked = new ArrayList<>();
        List<String> table2DataTimestampChecked = new ArrayList<>();
    
        for (String row2 : table2Data) {
            String value2 = getValue(row2);
            if (!uniqueValues.contains(value2)) {
                ArrayListTable2.add(row2);
                uniqueValues.add(value2);
            }
        }
    
        for (String row1 : table1Data) {
            for (String row2 : ArrayListTable2) {
                if (isTimestampWithinLimit(row1, row2, 10)) {
                    if (!table1DataTimestampChecked.contains(row1)) {
                        table1DataTimestampChecked.add(row1);
                    }
                    if (!table2DataTimestampChecked.contains(row2)) {
                        table2DataTimestampChecked.add(row2);
                    }
                }
            }
        }

        for (String row2 : table2DataTimestampChecked) {
            String value2 = getValue(row2);
            for (String row1 : table1DataTimestampChecked){
                String value1 = getValue(row1);
                if (value2.equals(value1)){
                    semiJoinedTable1DataTimestampFirst.add(row1);
                }
            }
        }
    
        return semiJoinedTable1DataTimestampFirst;
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