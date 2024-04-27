import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class Client {
	private String id;
	private int requestCounter;
	private Scanner in;
	private Map<String, Integer> servers;
	
	public Client() {
		id = String.valueOf(System.currentTimeMillis());
		requestCounter = 0;
		in = new Scanner(System.in);
		servers = new HashMap<>();
	}
	
	private int menu() {
		System.out.println("Enter your choice:");
		System.out.println("1. Register a server");
		System.out.println("2. Query");
		System.out.println("3. Update");
		System.out.println("4. Merge");
		System.out.println("0. Exit");
		int choice = in.nextInt();
		in.nextLine();
		return choice;
	}
	
	private void registerServer(String serverName, int port) {
		servers.put(serverName, port);
		System.out.println("Server " + serverName + " registered!");
	}
	
	private String chooseServer(String message) {
		if (servers.isEmpty()) {
	        System.out.println("No servers registered.");
	        return null;
	    }

	    System.out.println(message);
	    for (String serverName : servers.keySet()) {
	        System.out.println(serverName);
	    }
	    
	    String chosenServer = in.nextLine();
	    while (!servers.containsKey(chosenServer)) {
	        System.out.println("Invalid server name. Choose again:");
	        chosenServer = in.nextLine();
	    }
	    return chosenServer;
	}
	
	private String generateRequestId() {
		requestCounter += 1;
		return id + "_" + requestCounter;
	}
	
	private String sendQueryRequest(String server, String subject, String requestId) {
		int port = servers.get(server);
		try {
			URL url = new URL("http://localhost:"+ port);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("requestType", "query");
			connection.setRequestProperty("requestId", requestId);
			connection.setRequestProperty("requestParams", subject);
			int responseCode = connection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) { // success
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				return response.toString();
			} else {
				System.out.println("GET request did not work.");
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private String sendUpdateRequest(String server, String subject, String predicate, String object, String requestId) {
		int port = servers.get(server);
		try {
			URL url = new URL("http://localhost:"+ port);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("requestType", "update");
			connection.setRequestProperty("requestId", requestId);
			connection.setRequestProperty("requestParams", subject + "|" + predicate + "|" + object);
			int responseCode = connection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) { // success
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				return response.toString();
			} else {
				System.out.println("GET request did not work.");
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private String sendMergeRequest(String server1, String server2, String requestId) {
		int port1 = servers.get(server1);
		int port2 = servers.get(server2);
		try {
			URL url = new URL("http://localhost:"+ port1);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestProperty("requestType", "merge");
			connection.setRequestProperty("requestId", requestId);
			connection.setRequestProperty("requestParams", String.valueOf(port2));
			int responseCode = connection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) { // success
				BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String inputLine;
				StringBuffer response = new StringBuffer();
				while ((inputLine = in.readLine()) != null) {
					response.append(inputLine);
				}
				in.close();
				return response.toString();
			} else {
				System.out.println("GET request did not work.");
				return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public static void printTriples(List<Triple> triples) {
        System.out.println("Subject\t\tPredicate\t\tObject");
        System.out.println("----------------------------------------------------");
        for (Triple triple : triples) {
            String subject = triple.getSubject();
            String predicate = triple.getPredicate();
            String object = triple.getObject();
            System.out.printf("%-15s%-15s%-15s%n", subject, predicate, object);
        }
    }
	
	public static void main(String[] args) {
		System.out.println("Welcome!");
		Client client = new Client();
		int choice = 1;
		String server, server2;
		while(choice != 0) {
			choice = client.menu();
			switch (choice) {
			case 1:
				System.out.print("Enter sever nickname: ");
				String serverName = client.in.nextLine();
				System.out.print("Enter port number: ");
				int port = client.in.nextInt();
				client.in.nextLine();
				client.registerServer(serverName, port);
				break;
			case 2:
				server = client.chooseServer("Choose a server:");
				if (server != null) {
					System.out.print("Enter the subject you are querying for: ");
					String subject = client.in.nextLine();
					String requestId = client.generateRequestId();
					String result = client.sendQueryRequest(server, subject, requestId);
					if (result.equals("empty")) {
						System.out.println("No records found.");
					}
					else {
						List<Triple> results = Triple.stringToTriples(result);
						printTriples(results);
					}
				}
				break;
			case 3:
				server = client.chooseServer("Choose a server:");
				if (server != null) {
					System.out.print("Enter the subject of the triple you want to update: ");
					String subject = client.in.nextLine();
					System.out.print("Enter the predicate of the triple you want to update: ");
					String predicate = client.in.nextLine();
					System.out.print("Enter the object of the triple you want to update: ");
					String object = client.in.nextLine();
					String requestId = client.generateRequestId();
					String result = client.sendUpdateRequest(server, subject, predicate, object, requestId);
					if (result.equals("true")) {
						System.out.println("Update successful!");
					}
					else {
						System.out.println("Update failed :(");
					}
				}
				break;
			case 4:
				server = client.chooseServer("Choose server 1:");
				server2 = client.chooseServer("Choose a server 2:");
				String requestId = client.generateRequestId();
				if (server != server2) {
					String result = client.sendMergeRequest(server, server2, requestId);
					if (result.equals("true")) {
						System.out.println("Merge successful!");
					}
					else {
						System.out.println("Merge failed :(");
					}
				}
				else {
					System.out.println("Server 1 and 2 must be different");
				}
				break;
			case 0:
				System.out.println("Bye!");
			default:
				System.out.println("Invalid choice!");
				break;
			}
			System.out.println();
			System.out.println("----------------------------------------------------");
			System.out.println();
				
		}
	}


}