import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class HiveExecutor {

    private static final String DRIVER =
        "org.apache.hive.jdbc.HiveDriver";

    private static final String URL =
        "jdbc:hive2://localhost:10000/default";

    public static void main(String[] args) {

        if (args.length < 1) {

            System.out.println(
                "Usage: java HiveExecutor <hql_file>"
            );

            return;
        }

        String hqlFile = args[0];

        try {

            // Load JDBC driver
            Class.forName(DRIVER);

            // Connect to Hive
            Connection conn =
                DriverManager.getConnection(URL, "", "");

            System.out.println("✓ Connected to Hive");

            // Read HQL file
            String hql = new String(
                Files.readAllBytes(Paths.get(hqlFile))
            );

            // Split by semicolon
            String[] queries = hql.split(";");

            Statement stmt = conn.createStatement();

            for (String query : queries) {

                query = query.trim();

                if (query.isEmpty()) {
                    continue;
                }

                System.out.println("\nExecuting:");
                System.out.println(query);

                boolean hasResult = stmt.execute(query);

                // SELECT queries
                if (hasResult) {

                    ResultSet rs = stmt.getResultSet();

                    ResultSetMetaData meta =
                        rs.getMetaData();

                    int cols =
                        meta.getColumnCount();

                    System.out.println("\nResults:\n");

                    while (rs.next()) {

                        for (int i = 1; i <= cols; i++) {

                            System.out.print(
                                rs.getString(i)
                            );

                            if (i < cols) {
                                System.out.print("\t");
                            }
                        }

                        System.out.println();
                    }

                    rs.close();
                }

                System.out.println("\n✓ Success");
            }

            stmt.close();
            conn.close();

            System.out.println(
                "\n✓ HQL execution completed"
            );

        } catch (Exception e) {

            System.out.println(
                "\n✗ Hive execution failed"
            );

            e.printStackTrace();
        }
    }
}