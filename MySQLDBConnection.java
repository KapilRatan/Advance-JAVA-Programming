import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySQLDBConnection {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Create a connection to the database
        Connection conn;
        try {
            // Load the MySQL Connector/J driver
            Class.forName("com.mysql.cj.jdbc.Driver");
            String URL = "jdbc:mysql://localhost:3306/mydb";
            String User = "root";
            String Pass = "kapil@mysql";
            conn = DriverManager.getConnection(URL, User, Pass);
            if (conn == null) {
                System.out.println("Failed to connect!!!!");
            } else {
                System.out.println("Connection successful.....");
            }

            conn.close();

        } catch (ClassNotFoundException e) {
            System.out.println("Error loading driver");
            e.printStackTrace();

        } catch (SQLException e) {
            System.out.println("Error connecting to database");
            e.printStackTrace();
        }

    }
}