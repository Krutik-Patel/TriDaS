import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
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
		System.out.println("0. Exit");
		int choice = in.nextInt();
		in.nextLine();
		return choice;
	}
	
	private void registerServer(String serverName, int port) {
		servers.put(serverName, port);
		System.out.println("Server " + serverName + " registered!");
	}
	
	private String chooseServer() {
		if (servers.isEmpty()) {
	        System.out.println("No servers registered.");
	        return null;
	    }

	    System.out.println("Choose a server:");
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
	
	public static void main(String[] args) {
		System.out.println("Welcome!");
		Client client = new Client();
		int choice = 1;
		String server;
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
				server = client.chooseServer();
				if (server != null) {
					System.out.print("Enter the subject you are querying for: ");
					String subject = client.in.nextLine();
					String requestId = client.generateRequestId();
					String result = client.sendQueryRequest(server, subject, requestId);
					System.out.println(result);
				}
				break;
			case 3:
				server = client.chooseServer();
				if (server != null) {
					System.out.print("Enter the subject of the triple you want to update: ");
					String subject = client.in.nextLine();
					System.out.print("Enter the predicate of the triple you want to update: ");
					String predicate = client.in.nextLine();
					System.out.print("Enter the object of the triple you want to update: ");
					String object = client.in.nextLine();
					String requestId = client.generateRequestId();
					String result = client.sendUpdateRequest(server, subject, predicate, object, requestId);
					System.out.println(result);
				}
				break;
			}
				
		}
	}

	

	

}