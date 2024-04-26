import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class PostgreSQLServer extends Server {

	private final String url = "jdbc:postgresql://localhost/tridas";
	private final String user = "postgres";
	private final String password = "root";
	private Connection jdbcConnection;
	
	private void connect() {
		try(Connection connection = DriverManager.getConnection(url, user, password)) {
			if(connection != null) {
				System.out.println("Connected to PostgreSQL server successfully!");
				jdbcConnection = connection;
				createTripleTable(id);
			}
			else {
				System.out.println("Failed to connect PostgreSQL server");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void createTripleTable(String tableName) throws SQLException {
		DatabaseMetaData meta = jdbcConnection.getMetaData();
        ResultSet resultSet = meta.getTables(null, null, tableName, new String[] {"TABLE"});
        if (resultSet.next()) {
            System.out.println("Table " + tableName + " already exists.");
            System.out.println("WARNING: causalHistory for exitsting records not present");
        }
        else {
			Statement statement = jdbcConnection.createStatement();
			String sql = "CREATE TABLE " + tableName + " " +
		            "(SUBJECT	VARCHAR(100)," +
		            " PREDICATE	VARCHAR(100), " +
		            " OBJECT	VARCHAR(100), " +
		            " REQUESTID	VARCHAR(100), " +
		            " TIME	TIMESTAMP," + 
		            "PRIMARY KEY (SUBJECT, PREDICATE))";
		    statement.executeUpdate(sql);
		    System.out.println("Table " + tableName + " created...");
        }
	}
	
	public PostgreSQLServer(int port, String id) {
		super(port, id);
		connect();
	}

	@Override
	public List<Triple> query(String subject) {
		List<Triple> triples = new ArrayList<>();
		try {
			PreparedStatement selectStatement = jdbcConnection.prepareStatement("SELECT * FROM ? WHERE SUBECT=?");
			selectStatement.setString(1, id);
			selectStatement.setString(2, subject);
			ResultSet resultSet = selectStatement.executeQuery();
			while (resultSet.next()) {
                String resultSubject = resultSet.getString("SUBJECT");
                String predicate = resultSet.getString("PREDICATE");
                String object = resultSet.getString("OBJECT");
                String requestId = resultSet.getString("REQUESTID");
                Timestamp timestamp = resultSet.getTimestamp("TIME");
                LocalDateTime timeStamp = timestamp.toLocalDateTime();
                Triple triple = new Triple(resultSubject, predicate, object, requestId, timeStamp);
                triples.add(triple);
            }
            return triples;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}

	@Override
	public boolean update(String subject, String predicate, String object, String requestId) {
		
		PreparedStatement selectStatement;
		try {
			selectStatement = jdbcConnection.prepareStatement("SELECT * FROM " + id + " WHERE SUBJECT = ? AND PREDICATE = ?");
			selectStatement.setString(1, subject);
	        selectStatement.setString(2, predicate);
	        ResultSet resultSet = selectStatement.executeQuery();
	        
	        if (!resultSet.next()) {
                // Row doesn't exist, insert a new row
                PreparedStatement insertStatement = jdbcConnection.prepareStatement("INSERT INTO "+ id +" (SUBJECT, PREDICATE, OBJECT, REQEUSTID, TIME) VALUES (?, ?, ?, ?, ?)");
                insertStatement.setString(1, subject);
                insertStatement.setString(2, predicate);
                insertStatement.setString(3, object);
                insertStatement.setString(4, requestId);
                insertStatement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                insertStatement.executeUpdate();
                System.out.println("New record inserted: "+ subject+predicate+object);
            } else {
                PreparedStatement updateStatement = jdbcConnection.prepareStatement("UPDATE your_table SET OBJECT = ?, REQUESTID = ?, TIME = ? WHERE SUBJECT = ? AND PREDICATE = ?");
                updateStatement.setString(1, object);
                updateStatement.setString(2, requestId);
                updateStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                updateStatement.setString(4, subject);
                updateStatement.setString(5, predicate);
                updateStatement.executeUpdate();
                System.out.println("Record updated: "+ subject+predicate+object);
            }
	        return true;
	        
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}

	}

	@Override
	public boolean merge(String serverId, int otherPort) {
		// TODO Auto-generated method stub
		return false;
	}
	
	public static void main(String[] args) throws IOException {
		Scanner in = new Scanner(System.in);
		System.out.print("Enter port: ");
		int port = in.nextInt();
		in.nextLine();
		System.out.println("Enter server id: ");
		String id = in.nextLine();
		PostgreSQLServer server = new PostgreSQLServer(port, id);
		server.start();
	}
	
}