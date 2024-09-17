import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.management.ObjectName;

public class PreparedStatementMethod {
    static Connection connObj;

    // Method to establish connection to the database
    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://localhost:3306/mydbpreparedstat";
            String username = "root";
            String password = "kapil@mysql";
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connection established successfully.");
        } catch (SQLException e) {
            System.out.println("Error while connecting to the database: " + e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }

    // Method to create the Department table
    public boolean createTableDepartment(Connection conn) {
        String createTableDeptQuery = "CREATE TABLE IF NOT EXISTS DEPARTMENT(Did INT PRIMARY KEY, " +
                "Dname VARCHAR(70))";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableDeptQuery);
            System.out.println("Department table created successfully.");
            return true;
        } catch (SQLException e) {
            System.out.println("Error while creating Department table: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // Method to create the Employee table
    public boolean createTableEmployee(Connection conn) {
        String createTableEmpQuery = "CREATE TABLE IF NOT EXISTS EMPLOYEE (\n" +
                "Eid INT PRIMARY KEY, " +
                "Ename VARCHAR(30), " +
                "Salary DOUBLE, " +
                "Address VARCHAR(70), " +
                "Did INT, " +
                "FOREIGN KEY (Did) REFERENCES Department(Did))";
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTableEmpQuery);
            System.out.println("Employee table created successfully.");
            return true;
        } catch (SQLException e) {
            System.out.println("Error while creating Employee table: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    // Method to insert data using PreparedStatement and batch processing
    public void insertUsingPreparedStatementMethod(Connection conn) {
        System.out.println("Using Batch Processing to Insert multiple Prepared Statements");

        String insertDeptPreparedStatQuery = "INSERT INTO Department (Did, Dname) VALUES (?, ?)";
        String insertEmpPreparedStatQuery = "INSERT INTO Employee (Eid, Ename, Salary, Address, Did) VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement psDept = conn.prepareStatement(insertDeptPreparedStatQuery);
             PreparedStatement psEmp = conn.prepareStatement(insertEmpPreparedStatQuery)) {
             
            // Insert data into Department table using batch
            psDept.setInt(1, 2);
            psDept.setString(2, "Bachelor of Computer Application");
            psDept.addBatch();

            psDept.setInt(1, 1);
            psDept.setString(2, "MBBS");
            psDept.addBatch();

            psDept.setInt(1, 3);
            psDept.setString(2, "Computer Science");
            psDept.addBatch();

            psDept.setInt(1, 4);
            psDept.setString(2, "Research and Development");
            psDept.addBatch();

            psDept.setInt(1, 5);
            psDept.setString(2, "Mechanical");
            psDept.addBatch();

            // Insert data into Employee table using batch
            psEmp.setInt(1, 185);
            psEmp.setString(2, "Swayam");
            psEmp.setDouble(3, 50000);
            psEmp.setString(4, "Old Hubli");
            psEmp.setInt(5, 3);
            psEmp.addBatch();

            psEmp.setInt(1, 240);
            psEmp.setString(2, "Kapil");
            psEmp.setDouble(3, 45000);
            psEmp.setString(4, "Ashok Nagar");
            psEmp.setInt(5, 4);
            psEmp.addBatch();

            psEmp.setInt(1, 207);
            psEmp.setString(2, "Vijay");
            psEmp.setDouble(3, 60000);
            psEmp.setString(4, "Gabbur");
            psEmp.setInt(5, 4);
            psEmp.addBatch();

            psEmp.setInt(1, 623);
            psEmp.setString(2, "Khushi");
            psEmp.setDouble(3, 80000);
            psEmp.setString(4, "Gokul Road");
            psEmp.setInt(5, 1);
            psEmp.addBatch();

            // Execute the batch for both tables
            int[] deptBatchResults = psDept.executeBatch();
            int[] empBatchResults = psEmp.executeBatch();

            // Calculate total rows affected
            int totalRowsAffectedInEmp = 0;
            int totalRowsAffectedInDept = 0;

            for (int rowsOfDept : deptBatchResults) {
                totalRowsAffectedInDept += rowsOfDept;
            }

            for (int rowsOfEmp : empBatchResults) {
                totalRowsAffectedInEmp += rowsOfEmp;
            }

            System.out.println("Total number of rows inserted in DEPARTMENT: " + totalRowsAffectedInDept);
            System.out.println("Total number of rows inserted in EMPLOYEE: " + totalRowsAffectedInEmp);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Data inserted successfully.");
        }
    }

    public void displayScrollableResultsEmployee (Connection conn) {
        System.out.println("Displaying Employee data using a scrollable ResultSet:");
        String selectEmpQuery = "SELECT Eid, Ename, Salary, Address FROM EMPLOYEE";
        try (PreparedStatement ps = conn.prepareStatement(selectEmpQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rs = ps.executeQuery();

            // Scroll to the last row
            if (rs.last()) {
                System.out.println("Last Employee:");
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address"));
            }

            // Scroll to the first row
            if (rs.first()) {
                System.out.println("\nFirst Employee:");
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address"));
            }

            // Scroll through the result set
            System.out.println("\nScrolling through the result set:");
            rs.beforeFirst(); // Move to the beginning
            while (rs.next()) {
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address"));
            }

        } catch (SQLException e) {
            System.out.println("Error while fetching Employee data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void displayScrollableResultsDepartment(Connection conn) {
        System.out.println("Displaying Department data using a scrollable ResultSet:");
        String selectDeptQuery = "SELECT Did, Dname FROM DEPARTMENT";
        try (PreparedStatement ps = conn.prepareStatement(selectDeptQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
    
            ResultSet rs = ps.executeQuery();
    
            // Scroll to the last row
            if (rs.last()) {
                System.out.println("Last Department:");
                System.out.println("Did: " + rs.getInt("Did") + ", Dname: " + rs.getString("Dname"));
            }
    
            // Scroll to the first row
            if (rs.first()) {
                System.out.println("\nFirst Department:");
                System.out.println("Did: " + rs.getInt("Did") + ", Dname: " + rs.getString("Dname"));
            }
    
            // Scroll through the result set
            System.out.println("\nScrolling through the result set:");
            rs.beforeFirst(); // Move to the beginning
            while (rs.next()) {
                System.out.println("Did: " + rs.getInt("Did") + ", Dname: " + rs.getString("Dname"));
            }
    
        } catch (SQLException e) {
            System.out.println("Error while fetching Department data: " + e.getMessage());
            e.printStackTrace();
        }
    }
    

    public void displayScrollableResultsJoin(Connection conn) {
        System.out.println("Displaying joined Employee and Department data using a scrollable ResultSet:");
        String selectJoinQuery = "SELECT E.Eid, E.Ename, E.Salary, E.Address, D.Dname " +
                "FROM EMPLOYEE E " +
                "JOIN DEPARTMENT D ON E.Did = D.Did";
        try (PreparedStatement ps = conn.prepareStatement(selectJoinQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rs = ps.executeQuery();

            // Scroll to the last row
            if (rs.last()) {
                System.out.println("Last Employee:");
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address") +
                        ", Department: " + rs.getString("Dname"));
            }

            // Scroll to the first row
            if (rs.first()) {
                System.out.println("\nFirst Employee:");
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address") +
                        ", Department: " + rs.getString("Dname"));
            }

            // Scroll through the result set
            System.out.println("\nScrolling through the result set:");
            rs.beforeFirst(); // Move to the beginning
            while (rs.next()) {
                System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                        ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address") +
                        ", Department: " + rs.getString("Dname"));
            }
        } catch (SQLException e) {
            System.out.println("Error while fetching joined data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void displayEmployeeById(Connection conn) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Enter Employee ID to fetch details (or 'exit' to quit): ");
            String input = scanner.nextLine();

            if (input.equalsIgnoreCase("exit")) {
                break;
            }

            try {
                int empId = Integer.parseInt(input);
                String selectEmpByIdQuery = "SELECT Eid, Ename, Salary, Address FROM EMPLOYEE WHERE Eid = ?";
                try (PreparedStatement ps = conn.prepareStatement(selectEmpByIdQuery)) {
                    ps.setInt(1, empId);
                    ResultSet rs = ps.executeQuery();

                    if (rs.next()) {
                        System.out.println("Employee Details:");
                        System.out.println("Eid: " + rs.getInt("Eid") + ", Ename: " + rs.getString("Ename") +
                                ", Salary: " + rs.getDouble("Salary") + ", Address: " + rs.getString("Address"));
                    } else {
                        System.out.println("No employee found with ID: " + empId);
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid input. Please enter a valid Employee ID.");
            } catch (SQLException e) {
                System.out.println("Error while fetching employee details: " + e.getMessage());
                e.printStackTrace();
            }
        }
        scanner.close();
    }


    // Main method to execute the program
    public static void main(String[] args) {
        PreparedStatementMethod obj = new PreparedStatementMethod();  // Corrected class name here
        try {
            connObj = getConnection();
        } catch (ClassNotFoundException e) {
            System.out.println("Error while loading MySQL Connector/J: " + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Error while connecting to MySQL database: " + e.getMessage());
            e.printStackTrace();
        }

        // Create tables and insert data
        obj.createTableDepartment(connObj);
        obj.createTableEmployee(connObj);

        obj.insertUsingPreparedStatementMethod(connObj);  // Corrected method name here
        obj.displayScrollableResultsDepartment(connObj);
        obj.displayScrollableResultsEmployee(connObj);
        obj.displayEmployeeById(connObj);
    }
}
