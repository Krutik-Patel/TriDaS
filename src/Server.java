import java.util.List;
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

	protected String id;
	protected int port;
	protected HttpServer httpServer;
	protected Set<String> causalHistory;

	public abstract List<Triple> query(String subject);
	public abstract boolean update(String subject, String predicate, String object, String requestId);
	public abstract boolean merge(String serverId, int otherPort);

	public Server(int port, String id) {
		this.id = id;
		this.port = port;
		causalHistory = new HashSet<String>();
	}

	protected String triplesToString(List<Triple> triples) {
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
			List<String> requestTypes = requestHeaders.get("requestType");
			List<String> requestIds = requestHeaders.get("requestId");
			List<String> requestParams = requestHeaders.get("requestParams");
			String response = "Unkown request";
			System.out.println("RequestType: " + requestTypes);
			if (requestTypes != null) {
				String requestType = requestTypes.get(0);
				String requestId = requestIds.get(0);
				String requestParam = requestParams.get(0);
				if (requestType.equals("query")) {
					String subject = requestParam;
					List<Triple> result = query(subject);
					response = triplesToString(result);
				}
				else if (requestType.equals("update")) {
					System.out.println("Handling update");
					System.out.println("Request params: "+ requestParam);
					String subject = requestParam.split("\\|")[0];	
					String predicate = requestParam.split("\\|")[1];
					String object = requestParam.split("\\|")[2];
					System.out.println("Calling update with:" + subject + predicate + object);
					boolean result = update(subject, predicate, object, requestId);
					System.out.println("Response: "+ result);
					if (result) {
						causalHistory.add(requestId);
					}
					response = String.valueOf(result);
					
				}
				else if (requestType.equals("merge")) {
					String serverId = requestParams.get(0);
					int otherPort = Integer.parseInt(requestParams.get(1));
					boolean result = merge(serverId, otherPort);
					response = String.valueOf(result);
				}
			}
			exchange.sendResponseHeaders(200, response.getBytes().length);
        	OutputStream os = exchange.getResponseBody();
        	os.write(response.getBytes());
       		os.close();
		}
	}
	
	public void start() throws IOException {
		httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        	httpServer.createContext("/", new RequestHandler());
        	httpServer.setExecutor(null);
        	httpServer.start();
        	System.out.println("Server is running on port: " + port);
	}


}
