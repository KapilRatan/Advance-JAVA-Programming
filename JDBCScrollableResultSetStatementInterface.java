import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCScrollableResultSetStatementInterface {
    public static void main(String[] args) {
        Connection conn;
        try{
            // Load the MySQL Connector/J driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            String URL = "jdbc:mysql://localhost:3306/mydb";
            String User = "root";
            String Pass = "kapil@mysql";
            conn = DriverManager.getConnection(URL, User, Pass);
            if (conn == null){
                System.out.println("Failed to connect!!!!");
            } else {
                System.out.println("Connection successful.....");
            } 

            System.out.println("========================Scrollable Result Set=============================");

             // Create a Statement with scrollable ResultSet
            Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE, // Makes the ResultSet scrollable and sensitive to changes
                ResultSet.CONCUR_READ_ONLY       // Read-only ResultSet
            );

            String selectQuery = "select * from student";

            // Execute the query and get a scrollable ResultSet
            ResultSet rs = stmt.executeQuery(selectQuery);

            // Move the cursor to the last row and print it
            if (rs.last()) {
                System.out.println("Last row: " + rs.getInt("usn") + " " + rs.getString("name") + " " + rs.getString("degree"));
            }

            // Move the cursor to the first row and print it
            if (rs.first()) {
                System.out.println("First row: " + rs.getInt("usn") + " " + rs.getString("name") + " " + rs.getString("degree"));
            }

            // Move the cursor to a specific row (e.g., 3rd row) and print it
            if (rs.absolute(3)) {
                System.out.println("Third row: " + rs.getInt("usn") + " " + rs.getString("name") + " " + rs.getString("degree"));
            }


            boolean updateStatus = stmt.execute(selectQuery);
            System.out.println("Update status: "+ updateStatus);

            conn.close();

        } catch(ClassNotFoundException e){
            System.out.println("Error loading driver");
            e.printStackTrace();

        } catch(SQLException e){
            System.out.println("Error connecting to database");
            e.printStackTrace();

        }
    }
}
