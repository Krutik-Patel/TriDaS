import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.neo4j.driver.v1.*;

import static org.neo4j.driver.v1.Values.parameters;


public class Neo4JServer extends Server {
    private final String url = "bolt://localhost:7687";
    private final String username = "neo4j";
    private final String password = "password";
    private Driver driver;

    public void createTripleTable(String tableName) {
        try {
            driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
            System.out.println("Database connected...");
        } catch (Exception e) {
            System.out.println("Error connecting to Database ---- Trace: " + e.getMessage());
            driver.close();
        }
    }

    public Neo4JServer(int port, String id) {
        super(port, id);
        try {
            createTripleTable(id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Scanner in = new Scanner(System.in);
        System.out.println("Enter port: ");
        int port = in.nextInt();
        in.nextLine();
        System.out.println("Enter server id: ");
        String id = in.nextLine();
        Neo4JServer server = new Neo4JServer(port, id);
        server.start();
        in.close();
    }

    @Override
    public List<Triple> query(String subject) {
        List<Triple> triples = new ArrayList<>();
        try (Session session = driver.session()) {
            String query = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE s.value = $subject RETURN s, p, o";
            StatementResult result = session.run(query, parameters("subject", subject));
            while (result.hasNext()) {
                Record record = result.next();
                System.out.println(record.get("s").get("value").asString() + " " + record.get("p").get("value").asString() + " " + record.get("o").get("value").asString() + " " + record.get("p").get("requestID").asString() + " ");
                Value v = record.get("s").get("value");
                Value predicate = record.get("p").get("value");
                Value timestamp = record.get("p").get("timestamp");
                Value requestID = record.get("p").get("requestID");
                Value object = record.get("o").get("value");
                String subjectVal = v.asString();
                String pred = predicate.asString();
                String obj = object.asString();
                String reqID = requestID.asString();
                LocalDateTime timestampVal = LocalDateTime.parse(timestamp.asString());
                Triple triple = new Triple(subjectVal, pred, obj, reqID, timestampVal);
                triples.add(triple);
            }

        } catch (Exception e) {
            System.out.println("Error querying database ---- Trace: " + e.getMessage());
        }
        return triples; 
    }

    @Override
    public boolean update(String subject, String predicate, String object, String requestId) {
        try (Session session = driver.session()) {
            String query = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE s.value = $subject AND p.value = $predicate RETURN s, p, o";
            StatementResult result = session.run(query, parameters("subject", subject, "predicate", predicate));
            String currTime = LocalDateTime.now().toString();
            if (result.hasNext()) {
                String updateQuery = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE s.value = $subject AND p.value = $predicate SET p.timestamp = $timestamp, o.value = $object, p.requestID = $requestId";
                session.run(updateQuery, parameters("subject", subject, "predicate", predicate, "timestamp", currTime, "object", object, "requestId", requestId));
                System.out.println("Existing record updated...");
            } else {
                String insertQuery = "CREATE (s: SUBJECT {value: $subject})-[p:PREDICATE {value: $predicate, timestamp: $timestamp, requestID: $requestId}]->(o:OBJECT {value: $object})";
                session.run(insertQuery, parameters("subject", subject, "predicate", predicate, "object", object, "requestId", requestId, "timestamp", currTime));
                System.out.println("Created new record...");
            }
            return true;
        } catch (Exception e) {
            System.out.println("Error updating database ---- Trace: " + e.getMessage());
            return false;
        }
    }

    @Override
    protected List<Triple> getTriplesFromReqId(String reqid) {
        try (Session session = driver.session()) {
            String query = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE p.requestID = $reqid RETURN s, p, o";
            StatementResult result = session.run(query, parameters("reqid", reqid));
            List<Triple> triples = new ArrayList<>();
            while (result.hasNext()) {
                Record record = result.next();
                Value v = record.get("s").get("value");
                Value predicate = record.get("p").get("value");
                Value timestamp = record.get("p").get("timestamp");
                Value requestID = record.get("p").get("requestID");
                Value object = record.get("o").get("value");
                String subjectVal = v.asString();
                String pred = predicate.asString();
                String obj = object.asString();
                String reqID = requestID.asString();
                LocalDateTime timestampVal = LocalDateTime.parse(timestamp.asString());
                Triple triple = new Triple(subjectVal, pred, obj, reqID, timestampVal);
                triples.add(triple);
            }
            return triples;

        } catch (Exception e) {
            System.out.println("Error getting triples from request id ---- Trace: " + e.getMessage());
            return null;
        }

    }

    @Override
    protected boolean compareAndUpdate(Triple triple) {
        try (Session session = driver.session()) {
            String query = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE s.value = $subject AND p.value = $predicate return s, p, o";
            StatementResult result = session.run(query, parameters("subject", triple.getSubject(), "predicate", triple.getPredicate()));
            System.out.println("place 1 done");
            if (result.hasNext()) {
                Record record = result.next();
                Value timestamp = record.get("p").get("timestamp");
                LocalDateTime timestampVal = LocalDateTime.parse(timestamp.asString());
                if (triple.getTimeStamp().isAfter(timestampVal)) {
                    String updateQuery = "MATCH (s: SUBJECT)-[p:PREDICATE]->(o:OBJECT) WHERE s.value = $subject AND p.value = $predicate SET p.timestamp = $timestamp, o.value = $object, p.requestID = $requestId";
                    session.run(updateQuery, parameters("subject", triple.getSubject(), "predicate", triple.getPredicate(), "timestamp", triple.getTimeStamp().toString(), "object", triple.getObject(), "requestId", triple.getRequestId()));
                    System.out.println("place 2 done");
                }
                return true;
            } else {
                String insertQuery = "CREATE (s: SUBJECT {value: $subject})-[p:PREDICATE {value: $predicate, timestamp: $timestamp, requestID: $requestId}]->(o:OBJECT {value: $object})";
                session.run(insertQuery, parameters("subject", triple.getSubject(), "predicate", triple.getPredicate(), "object", triple.getObject(), "requestId", triple.getRequestId(), "timestamp", triple.getTimeStamp().toString()));
                System.out.println(triple.getSubject() + triple.getPredicate() + triple.getObject() + triple.getRequestId() + triple.getTimeStamp().toString());
                return true;
            }

        } catch (Exception e) {
            System.out.println("Error comparing and updating ---- Trace: " + e.getMessage());
            return false;
        }
    }
}
