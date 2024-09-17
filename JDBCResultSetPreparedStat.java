import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JDBCResultSetPreparedStat {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // Create a connection to the database
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
            
            //System.out.println("====================================Statement========================================");
        
            // String tableQuery = "create table student(usn int primary key, name varchar(20), degree varchar(20))";
            // int tableCreated = stmt.executeUpdate(tableQuery);
            // System.out.println("Table created successfully.");
            
            //System.out.println("==============================Prepared Statement======================================");

            String preparedInsertQuery = "insert into student(usn, name, degree) values(?,?,?)";
            PreparedStatement pstmt = conn.prepareStatement(preparedInsertQuery);
            pstmt.setInt(1, 185);
            pstmt.setString(2, "Swayam");
            pstmt.setString(3, "BCA");

            String preparedSelectQuery = "select * from student";
            pstmt = conn.prepareStatement(preparedSelectQuery);
            

            //System.out.println("==============================Result Set======================================");
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                System.out.println(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3));
            }

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
