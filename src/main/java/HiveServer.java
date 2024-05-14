import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


public class HiveServer extends Server {

	private final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private final String HIVE_URL = "jdbc:hive2://localhost:10000"; // Hive JDBC connection URL
    private Connection connection;
	
	
    private void createTripleTable(String tableName) throws SQLException {
		try {
			Class.forName(DRIVER_NAME);
		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found!");
			e.printStackTrace();
		}
		connection = DriverManager.getConnection(HIVE_URL);
		System.out.println("Connected to Hive server successfully!");
        
        try {
            Statement stmt1 = connection.createStatement();
			stmt1.execute("SET hive.exec.dynamic.partition = true");
			stmt1.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
			stmt1.execute("SET hive.exec.max.dynamic.partitions = 10000");
			stmt1.execute("SET hive.exec.max.dynamic.partitions.pernode = 1000");
			stmt1.execute("SET hive.support.concurrency = true");
			stmt1.execute("SET hive.enforce.bucketing = true");
			stmt1.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
			stmt1.execute("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
			stmt1.execute("SET hive.compactor.initiator.on = true");
			stmt1.execute("SET hive.compactor.worker.threads = 1");

            // Check if the table already exists
            ResultSet tables = stmt1.executeQuery("SHOW TABLES LIKE '" + tableName + "'");
            
            if (tables.next()) {
                System.out.println("Table " + tableName + " already exists.");
                
                Statement stmt = connection.createStatement();
                ResultSet result = stmt.executeQuery("SELECT DISTINCT requestId FROM " + tableName);
                while (result.next()) {
                    causalHistory.add(result.getString("requestId"));
                }
                
                stmt.close();
                System.out.println("Causal history populated with "+causalHistory.size() + " entries.");
            }

            else {
            	// Create a new table
            	Statement stmt = connection.createStatement();

				stmt.execute("SET hive.exec.dynamic.partition = true");
				stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
				stmt.execute("SET hive.exec.max.dynamic.partitions = 10000");
				stmt.execute("SET hive.exec.max.dynamic.partitions.pernode = 1000");
				stmt.execute("SET hive.support.concurrency = true");
				stmt.execute("SET hive.enforce.bucketing = true");
				stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
				stmt.execute("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
				stmt.execute("SET hive.compactor.initiator.on = true");
				stmt.execute("SET hive.compactor.worker.threads = 1");

				String createTableQuery = "CREATE TABLE " + tableName + " ("
                        + "subject STRING, "
                        + "predicate STRING, "
                        + "object STRING, "
                        + "requestId STRING, "
                        + "time_stamp TIMESTAMP"
                        + ")";
                stmt.execute(createTableQuery);
                System.out.println("Table " + tableName + " created.");

                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        finally {
        	connection.close();
        }
	}
	
	public HiveServer(int port, String id) {
		super(port, id);
		try {
			createTripleTable(id);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Triple> query(String subject) {
		List<Triple> triples = new ArrayList<>();
		try {
			connection = DriverManager.getConnection(HIVE_URL);
	        PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM " + id + " WHERE subject=?");
	        selectStatement.setString(1, subject);
	        ResultSet resultSet = selectStatement.executeQuery();
	        
	        resultSetToList(triples, resultSet);
	        
	        connection.close();
	        return triples;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}

	private void resultSetToList(List<Triple> triples, ResultSet resultSet) throws SQLException {
		while (resultSet.next()) {
			String resultSubject = resultSet.getString("subject");
	        String predicate = resultSet.getString("predicate");
	        String object = resultSet.getString("object");
	        String requestId = resultSet.getString("requestId");
	        Timestamp timestamp = resultSet.getTimestamp("time_stamp");
	        LocalDateTime timeStamp = timestamp.toLocalDateTime();
	        Triple triple = new Triple(resultSubject, predicate, object, requestId, timeStamp);
	        triples.add(triple);
		}
	}

	@Override
	public boolean update(String subject, String predicate, String object, String requestId) {
		try {
	        connection = DriverManager.getConnection(HIVE_URL);
	        
	        PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM " + id + " WHERE subject = ? AND predicate = ?");
	        selectStatement.setString(1, subject);
	        selectStatement.setString(2, predicate);
	        ResultSet resultSet = selectStatement.executeQuery();
	        
	        if (!resultSet.next()) {
	            // Row doesn't exist, insert a new row
	            PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO " + id + " (subject, predicate, object, requestId, time_stamp) VALUES (?, ?, ?, ?, ?)");
	            
	            insertStatement.setString(1, subject);
	            insertStatement.setString(2, predicate);
	            insertStatement.setString(3, object);
	            insertStatement.setString(4, requestId);
	            insertStatement.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
	            
	            insertStatement.executeUpdate();
	            
	            System.out.println("New record inserted: " + subject + predicate + object);
	        } else {
	            // Row exists, update the existing row
	            PreparedStatement updateStatement = connection.prepareStatement("UPDATE " + id + " SET object = ?, requestId = ?, time_stamp = ? WHERE subject = ? AND predicate = ?");
	            
	            updateStatement.setString(1, object);
	            updateStatement.setString(2, requestId);
	            updateStatement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
	            updateStatement.setString(4, subject);
	            updateStatement.setString(5, predicate);
	            
	            updateStatement.executeUpdate();
	            
	            System.out.println("Record updated: " + subject + predicate + object);
	        }
	        
	        connection.close();
	        return true;
	    } catch (SQLException e) {
	        e.printStackTrace();
	        return false;
	    }
	}

	@Override
	protected List<Triple> getTriplesFromReqId(String reqid) {
		List<Triple> triples = new ArrayList<>();
		try {
			connection = DriverManager.getConnection(HIVE_URL);
	        PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM " + id + " WHERE requestId=?");
	        selectStatement.setString(1, reqid);
	        ResultSet resultSet = selectStatement.executeQuery();
	        
	        resultSetToList(triples, resultSet);
	        
	        connection.close();
	        return triples;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	protected boolean compareAndUpdate(Triple triple) {
		try {
	        connection = DriverManager.getConnection(HIVE_URL);
	        
	        PreparedStatement selectStatement = connection.prepareStatement("SELECT * FROM " + id + " WHERE subject = ? AND predicate = ?");
	        selectStatement.setString(1, triple.getSubject());
			selectStatement.setString(2, triple.getPredicate());
	        ResultSet resultSet = selectStatement.executeQuery();
	        
	        if (!resultSet.next()) {
	            // Row doesn't exist, insert a new row
	            PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO " + id + " (subject, predicate, object, requestId, time_stamp) VALUES (?, ?, ?, ?, ?)");
	            
	            insertStatement.setString(1, triple.getSubject());
                insertStatement.setString(2, triple.getPredicate());
                insertStatement.setString(3, triple.getObject());
                insertStatement.setString(4, triple.getRequestId());
                insertStatement.setTimestamp(5, Timestamp.valueOf(triple.getTimeStamp()));
                
	            insertStatement.executeUpdate();
	            
	            System.out.println("New record inserted: " + triple.getSubject()+triple.getPredicate()+triple.getObject());
	        } else {
	        	Timestamp existingTime = resultSet.getTimestamp("time");
            	Timestamp mergeTime = Timestamp.valueOf(triple.getTimeStamp());
            	if (existingTime.before(mergeTime)) {
            		PreparedStatement updateStatement = connection.prepareStatement("UPDATE " + id + " SET object = ?, requestId = ?, time_stamp = ? WHERE subject = ? AND predicate = ?");
            		
            		updateStatement.setString(1, triple.getObject());
            		updateStatement.setString(2, triple.getRequestId());
            		updateStatement.setTimestamp(3, mergeTime);
            		updateStatement.setString(4,  triple.getSubject());
            		updateStatement.setString(5, triple.getPredicate());
            		
            		updateStatement.executeUpdate();
            		
            		System.out.println("Record updated: "+ triple.getSubject()+triple.getPredicate()+triple.getObject());
            	}
	        }
	        
	        connection.close();
	        return true;
	    } catch (SQLException e) {
	        e.printStackTrace();
	        return false;
	    }

	}
	
	public static void main(String[] args) throws IOException {
		Scanner in = new Scanner(System.in);
		System.out.print("Enter port: ");
		int port = in.nextInt();
		in.nextLine();
		System.out.println("Enter server id: ");
		String id = in.nextLine();
		HiveServer server = new HiveServer(port, id);
		server.start();
	}

	
}
