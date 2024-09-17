import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;


public class StatementMethod {
    static Connection connObj;

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        try {
            String url = "jdbc:mysql://localhost:3306/mydbstatmethod";
            String username = "root";
            String password = "kapil@mysql";
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connection established successfully.");
        } catch (SQLException e) {
            System.out.println("Error while connecting to database: " + e.getMessage());
            e.printStackTrace();
        }
        return conn;
    }

    public boolean createTableDepartment(Connection conn) {
        String createTableDeptQuery = "CREATE TABLE IF NOT EXISTS DEPARTMENT(Did INT PRIMARY KEY, " +
                "Dname VARCHAR(70))";
        try (PreparedStatement ps = conn.prepareStatement(createTableDeptQuery)) {
            ps.executeUpdate();
            System.out.println("Department tables created successfully.");
            return true;
        } catch (SQLException e) {
            System.out.println("Error while creating Department table: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    public boolean creatTableEmployee(Connection conn) {
        String createTableEmpQuery = "CREATE TABLE IF NOT EXISTS EMPLOYEE (\n" +
                "Eid INT PRIMARY KEY, " +
                "Ename VARCHAR(30), " +
                "Salary DOUBLE, " +
                "Address VARCHAR(70), " +
                "Did INT, " +
                "FOREIGN KEY (Did) REFERENCES Department(Did))";
        try (PreparedStatement ps = conn.prepareStatement(createTableEmpQuery)) {
            ps.executeUpdate();
            System.out.println("Employee tables created successfully.");
            return true;
        } catch (SQLException e) {
            System.out.println("Error while creating Department table: " + e.getMessage());
            e.printStackTrace();
            return false;
        }

    }

    public void insertUsingStatementMethod(Connection conn) {
        System.out.println("Inserting using Statement()");
        String insertDeptStatementQuery01 = "INSERT INTO DEPARTMENT(Did, Dname) VALUES (1, 'MBBS')";
        try (Statement stmt01 = conn.createStatement()) {
            int noOfrowsEffected = stmt01.executeUpdate(insertDeptStatementQuery01);
            System.out.println("Number of rows affected: " + noOfrowsEffected);

        } catch (SQLException e) {
            System.out.println("Error while inserting Department table: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Data inserted successfully.");

        }

        System.out.println("Using Batch processing to Insert many rows using Statement()");

        String insertDeptStatementQuery02 = "INSERT INTO DEPARTMENT(Did, Dname) VALUES (2, 'Bachelor of Computer Application')";
        String insertDeptStatementQuery03 = "INSERT INTO DEPARTMENT(Did, Dname) VALUES (2, 'R & D')";
        String insertEmpStatementQuery01 = "INSERT INTO EMPLOYEE(Eid, Ename, Salary, Address, Did) VALUES (240, 'Kapil', 60000.00, 'Ashok Nagar', 2)";
        String insertEmpStatementQuery02 = "INSERT INTO EMPLOYEE(Eid, Ename, Salary, Address, Did) VALUES (218, 'Rachit', 200000.00, 'Ashok Nagar', 1)";
        String insertEmpStatementQuery03 = "INSERT INTO EMPLOYEE(Eid, Ename, Salary, Address, Did) VALUES (240, 'Aakash', 500000.00, 'Ranebennur, Near railway station', 3)";

        try (Statement stmt02 = conn.createStatement()) {

            stmt02.addBatch(insertDeptStatementQuery02);
            stmt02.addBatch(insertDeptStatementQuery03);
            stmt02.addBatch(insertEmpStatementQuery01);
            stmt02.addBatch(insertEmpStatementQuery02);
            stmt02.addBatch(insertEmpStatementQuery03);

            // Execute the batch
            int[] noOfrowsEffected = stmt02.executeBatch();

            // Calculate total rows affected
            int totalRowsAffected = 0;
            for (int countRows : noOfrowsEffected) {
                totalRowsAffected += countRows;
            }

            System.out.println("Total number of rows affected: " + totalRowsAffected);

        } catch (SQLException e) {
            System.out.println("Error while inserting into Department table: " + e.getMessage());
        } finally {
            System.out.println("Data inserted successfully.");

        }
    }

    public void displayScrollableResultsEmployee(Connection conn) {
        System.out.println("Displaying Employee data using a scrollable ResultSet:");
        String selectEmpQuery = "SELECT Eid, Ename, Salary, Address FROM EMPLOYEE";
        try (Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
    
            ResultSet rs = stmt.executeQuery(selectEmpQuery);
    
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
        try (Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
    
            ResultSet rs = stmt.executeQuery(selectDeptQuery);
    
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
        try (Statement stmt = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
    
            ResultSet rs = stmt.executeQuery(selectJoinQuery);
    
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
                String selectEmpByIdQuery = "SELECT Eid, Ename, Salary, Address FROM EMPLOYEE WHERE Eid = " + empId;
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(selectEmpByIdQuery);
    
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
    
    public static void main(String[] args) {

        StatementMethod obj = new StatementMethod();
        try {
            connObj = getConnection();
        } catch (ClassNotFoundException e) {
            System.out.println("Error while loading MySQL Connector/J: " + e.getMessage());
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Error while connecting to MySQL database: " + e.getMessage());
            e.printStackTrace();
        }

        obj.createTableDepartment(connObj);
        obj.creatTableEmployee(connObj);

        obj.insertUsingStatementMethod(connObj);

        obj.displayScrollableResultsEmployee(connObj);
        obj.displayScrollableResultsDepartment(connObj);

        obj.displayScrollableResultsJoin(connObj);

        obj.displayEmployeeById(connObj);

    }
}