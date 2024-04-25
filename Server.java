package TriDaS;

import TriDaS.Triple;
import java.util.List;
import java.util.Scanner;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public abstract class Server {

	private String id;
	private int port;
	private HttpServer httpServer;

	public abstract List<Triple> query(String subject);
	public abstract boolean update(String subject, String predicate);
	public abstract boolean merge(String serverId);

	class RequestHandler implements HttpHandler {
		public void handle(HttpExchange exchange) throws IOException {
			String response = "mic testing. haalloo";
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
