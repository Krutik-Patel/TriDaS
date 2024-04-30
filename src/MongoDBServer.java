import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;

public class MongoDBServer extends Server {

	private final String url = "localhost";
	private String databaseName = "tridas_mongoDB";
	private MongoClient mongoClient;
	
	
	private void createTripleTable(String tableName) throws SQLException {
		// Creating a Mongo client
		mongoClient = new MongoClient(url, port);
		
		// Accessing the database
		MongoDatabase database = mongoClient.getDatabase(databaseName);
		
		// Retrieving a collection (similar to table in RDBMS, but Schemaless)
		MongoCollection<Document> records = database.getCollection(tableName);
		
        if (records != null) {
            System.out.println("Table " + tableName + " already exists.");
            
            // Get distinct RequestIds and add them to causal history of server
            List<String> distinctRequestIds = records.distinct("requestId", String.class).into(new ArrayList<>());
            for (String requestId : distinctRequestIds) {
                causalHistory.add(requestId);
            }
            
            System.out.println("Causal history populated with "+causalHistory.size() + " entries.");
        }
        else {
			//Create new collection/table
        	database.createCollection(tableName);
        	
		    System.out.println("Table " + tableName + " created...");
        }
       
        //Closing a Mongo Client
		mongoClient.close();
	}
	
	public MongoDBServer(int port, String id) {
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
			// Creating a Mongo client
			mongoClient = new MongoClient(url, port);
			
			// Accessing the database
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			
			// Retrieving a collection (similar to table in RDBMS, but Schemaless)
			MongoCollection<Document> records = database.getCollection(id);
			
			// Define the query criteria
	        Document query = new Document("subject", subject);

	        //Documents : similar to rows in RDBMS, but stored in BSON(binary JSON) into the collection from file
	        // Find documents matching the query
	        FindIterable<Document> result = collection.find(query);
	        
	        // Converting Documents List to Triples List
			docListToTripleList(result, triples);
	        
			//Closing a Mongo Client
			mongoClient.close();
			
            return triples;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
	}
	
	private void docListToTripleList(FindIterable<Document> result, List<Triple> triples) {
        for (Document document : result) {
        	// Extracting Information from document
            String subject = document.getString("subject");
            String predicate = document.getString("predicate");
            String object = document.getString("object");
            String requestId = document.getString("requestId");
            Date date = document.getDate("timeStamp");
            LocalDateTime timeStamp = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
            
            // Creating new Triple add adding it to triples list
            Triple triple = new Triple(subject, predicate, object, requestId, timeStamp);
            triples.add(triple);
        }
    }

	@Override
	public boolean update(String subject, String predicate, String object, String requestId) {
		try {
			// Creating a Mongo client
			mongoClient = new MongoClient(url, port);
			
			// Accessing the database
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			
			// Retrieving a collection (similar to table in RDBMS, but Schemaless)
			MongoCollection<Document> records = database.getCollection(id);	
            
			// Define the query criteria
	        Document query = new Document("subject", subject).append("predicate", predicate);

	        // Define the update operation
	        // "$set" is used to update the value of document field
	        Document update = new Document("$set", new Document("object", object).append("requestId", requestId).append("timeStamp", new Date()));

	        // Perform the update operation
	        // query is key to search, update is new value
	        // UpdateOptions.upsert(true) -- checks if document exists or not, if exists then update the document otherwise creates new document
	        collection.updateOne(query, update, new UpdateOptions().upsert(true));
			
	        System.out.println("Record updated: "+ subject+predicate+object);
	        
	        //Closing a Mongo Client
			mongoClient.close();
			
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
			// Creating a Mongo client
			mongoClient = new MongoClient(url, port);
			
			// Accessing the database
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			
			// Retrieving a collection (similar to table in RDBMS, but Schemaless)
			MongoCollection<Document> records = database.getCollection(id);
			
			// Define the query criteria
			Document query = new Document("requestId", reqid);

	        //Documents : similar to rows in RDBMS, but stored in BSON(binary JSON) into the collection from file
	        // Find documents matching the query
	        FindIterable<Document> result = collection.find(query);
	        
	        // Converting Documents List to Triples List
			docListToTripleList(result, triples);
	        
			//Closing a Mongo Client
			mongoClient.close();
						
            return triples;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	@Override
	protected boolean compareAndUpdate(Triple triple) {
		try {
			// Creating a Mongo client
			mongoClient = new MongoClient(url, port);
			
			// Accessing the database
			MongoDatabase database = mongoClient.getDatabase(databaseName);
			
			// Retrieving a collection (similar to table in RDBMS, but Schemaless)
			MongoCollection<Document> records = database.getCollection(id);	
            
			// Define the query criteria
	        Document query = new Document("subject", triple.getSubject()).append("predicate", triple.getPredicate());

	        // Define the update operation
	        // "$set" is used to update the value of document field
	        Document update = new Document("$set", new Document("object", triple.getObject()).append("requestId", triple.getRequestId()).append("timeStamp", new Date()));

	        // Perform the update operation
	        // query is key to search, update is new value
	        // UpdateOptions.upsert(true) -- checks if document exists or not, if exists then update the document otherwise creates new document
	        collection.updateOne(query, update, new UpdateOptions().upsert(true));
			
	        System.out.println("Record updated: "+ triple.getSubject()+triple.getPredicate()+triple.getObject());

	        //Closing a Mongo Client
			mongoClient.close();
			
	        return true;
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
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
		MongoDBServer server = new MongoDBServer(port, id);
		server.start();
	}

	
}
