package TriDaS;

import TriDaS.Triple;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.HashSet;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Headers;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public abstract class Server {

	private String id;
	private int port;
	private HttpServer httpServer;
	private Set<String> causalHistory;

	public abstract List<Triple> query(String subject);
	public abstract boolean update(String subject, String predicate);
	public abstract boolean merge(String serverId, int otherPort);

	public Server() {
		causalHistory = new HashSet<String>();
	}

	private String triplesToString(List<Triple> triples) {
		String result = "";
		for (Triple triple: triples) {
			result += triple.toString() + "|";
		}
		result = result.substring(0, result.length()-1);
		return result;
	}

	class RequestHandler implements HttpHandler {
		public void handle(HttpExchange exchange) throws IOException {
			Headers requestHeaders = exchange.getRequestHeaders();
			String requestType = requestHeaders.get("requestType").get(0);
			String requestId = requestHeaders.get("requestId").get(0);
			String requestParams = requestHeaders.get("requestParams").get(0);
			String response = "";
			if (requestType.equals("query")) {
				String subject = requestParams;
				List<Triple> result = query(subject);
				response = triplesToString(result);
			}
			else if (requestType.equals("update")) {
				String subject = requestParams.split(",")[0];	
				String predicate = requestParams.split(",")[1];	
				boolean result = update(subject, predicate);
				response = String.valueOf(result);
			}
			else if (requestType.equals("merge")) {
				String serverId = requestParams.split(",")[0];
				int otherPort = Integer.parseInt(requestParams.split(",")[1]);
				boolean result = merge(serverId, otherPort);
				response = String.valueOf(result);
			}
			exchange.sendResponseHeaders(200, response.getBytes().length);
        		OutputStream os = exchange.getResponseBody();
        		os.write(response.getBytes());
       			os.close();
		}
	}
	
	public void start(int port, String id) throws IOException {
		this.id = id;
		this.port = port;
		httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        	httpServer.createContext("/", new RequestHandler());
        	httpServer.setExecutor(null);
        	httpServer.start();
        	System.out.println("Server is running on port: " + port);
	}


}
