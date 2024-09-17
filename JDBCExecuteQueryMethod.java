import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCExecuteQueryMethod {
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

            Statement stmt = conn.createStatement();
            
            

            // String tableQuery = "create table student(usn int primary key, name varchar(20), degree varchar(20))";
            // int tableCreated = stmt.executeUpdate(tableQuery);
            // System.out.println("Student table created successfully.");
            

            //String insertQuery = "insert into student values(240, 'Kapil', 'BCA')";
            //String insertQuery = "insert into student values(101, 'Aakash', 'ME')";
            // int insertData = stmt.executeUpdate(insertQuery);
            // System.out.println("Number of rows effected: " + insertData);

            String selectQuery = "select * from student";

            int totalNumberOfRows = stmt.executeUpdate(selectQuery);
            System.out.println("Total number of rows: "+ totalNumberOfRows);

            System.out.println("==============================Execute Query Method======================================");

            ResultSet rs = stmt.executeQuery(selectQuery);
            while(rs.next()){
                System.out.println(rs.getInt(1) + " " + rs.getString(2)+ " " + rs.getString(3));
            }

            boolean isSuccess = stmt.execute(selectQuery);
            System.out.println("Query execution status: "+ isSuccess);

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
