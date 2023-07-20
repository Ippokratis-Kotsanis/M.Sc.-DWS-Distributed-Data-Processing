import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Random;

public class twoDatabases {

    public static void main(String[] args) {
        Connection connection1 = null;
        try {
            connection1 = DriverManager.getConnection("jdbc:sqlite:/volumes/database1/database1.db");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

        Connection connection2 = null;
        try {
            connection2 = DriverManager.getConnection("jdbc:sqlite:/volumes/database2/database2.db");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }

        createSQLiteTable(connection1, "table1");
        createSQLiteTable(connection1, "table2");
        createSQLiteTable(connection2, "table1");
        createSQLiteTable(connection2, "table2");

        clearSQLiteTable(connection1, "table1");
        clearSQLiteTable(connection1, "table2");
        clearSQLiteTable(connection2, "table1");
        clearSQLiteTable(connection2, "table2");

        Random random = new Random(3136); // Set seed value
        for (int i = 0; i < 50; i++) {
            String recordId = "record" + i;
            String timestamp = String.valueOf(System.currentTimeMillis());
            String value1 = String.valueOf(random.nextInt(100));
            String value2 = String.valueOf(random.nextInt(90));
            String value3 = String.valueOf(random.nextInt(110));
            String value4 = String.valueOf(random.nextInt(95));

            if (i < 20) {
                insertSQLiteRecord(connection1, "table1", recordId, value1, timestamp);
            }

            if (i < 30) {
                insertSQLiteRecord(connection1, "table2", recordId, value2, timestamp);
            }

            if (i < 40) {
                insertSQLiteRecord(connection2, "table1", recordId, value3, timestamp);
            }

            insertSQLiteRecord(connection2, "table2", recordId, value4, timestamp);
        }

        try {
            connection1.close();
            connection2.close();
            System.out.println("Program was run successfully!");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    private static void createSQLiteTable(Connection connection, String tableName) {
        try {
            Statement statement = connection.createStatement();
            String query = "CREATE TABLE IF NOT EXISTS " + tableName + " (record_id TEXT, value TEXT, timestamp TEXT)";
            statement.executeUpdate(query);
            statement.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    private static void clearSQLiteTable(Connection connection, String tableName) {
        try {
            Statement statement = connection.createStatement();
            String query = "DELETE FROM " + tableName;
            statement.executeUpdate(query);
            statement.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    private static void insertSQLiteRecord(Connection connection, String tableName, String recordId, String value, String timestamp) {
        try {
            Statement statement = connection.createStatement();
            String query = "INSERT INTO " + tableName + " (record_id, value, timestamp) VALUES ('" + recordId + "', '" + value + "', '" + timestamp + "')";
            statement.executeUpdate(query);
            statement.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
