import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Headers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

public abstract class Server {

	protected String id;
	protected int port;
	protected HttpServer httpServer;
	protected Set<String> causalHistory;

	public abstract List<Triple> query(String subject);
	public abstract boolean update(String subject, String predicate, String object, String requestId);
	protected abstract List<Triple> getTriplesFromReqId(String reqid);
	protected abstract boolean compareAndUpdate(Triple triple);
	
	public boolean merge(int otherPort) {
		URL url;
		HttpURLConnection connection;
		try {
			url = new URL("http://localhost:"+ otherPort);
			connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("POST");
			connection.setRequestProperty("requestType", "serverMerge");
			connection.setRequestProperty("requestId", id);
			connection.setRequestProperty("requestParams", id);
			
			connection.setDoOutput(true);
			OutputStream os = connection.getOutputStream();
			os.write(causalHistory.toString().getBytes());
			os.flush();
			os.close();
			
			int responseCode = connection.getResponseCode();
			BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
			
			String diffString = response.toString();
			Set<String> diff = parseCausalHistory(diffString);
			
			List<Triple> diffTriples = new ArrayList<>();
			for (String reqid: diff) {
				List<Triple> triplesOfId = getTriplesFromReqId(reqid);
				diffTriples.addAll(triplesOfId);
			}
			boolean success = true;
			for (Triple triple: diffTriples) {
				success = compareAndUpdate(triple) && success;
			}
			if(success) {
				causalHistory.addAll(diff);
				return true;
			}
			else {
				return false;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
	}

	
	public Server(int port, String id) {
		this.id = id;
		this.port = port;
		causalHistory = new HashSet<String>();
	}
	
	public static Set<String> parseCausalHistory(String input){
		Set<String> otherCausalHistory = new HashSet<>();
		input = input.substring(1, input.length()-1);
		String[] ids = input.split(",");
		for (String id : ids) {
			otherCausalHistory.add(id);
		}
		return otherCausalHistory;
	}


	class RequestHandler implements HttpHandler {
		public void handle(HttpExchange exchange) throws IOException {
			Headers requestHeaders = exchange.getRequestHeaders();
			List<String> requestTypes = requestHeaders.get("requestType");
			List<String> requestIds = requestHeaders.get("requestId");
			List<String> requestParams = requestHeaders.get("requestParams");
			String response = "Unkown request";
			
			if (requestTypes != null) {
				String requestType = requestTypes.get(0);
				String requestId = requestIds.get(0);
				String requestParam = requestParams.get(0);
				System.out.println("RequestId: " + requestId);
				System.out.println("RequestType: " + requestType);
				if (requestType.equals("query")) {
					String subject = requestParam;
					List<Triple> result = query(subject);
					response = Triple.triplesToString(result);
				}
				else if (requestType.equals("update")) {
					
					String subject = requestParam.split("\\|")[0];	
					String predicate = requestParam.split("\\|")[1];
					String object = requestParam.split("\\|")[2];
					boolean result = update(subject, predicate, object, requestId);
					if (result) {
						causalHistory.add(requestId);
					}
					response = String.valueOf(result);
					
				}
				else if (requestType.equals("merge")) {
					int otherPort = Integer.parseInt(requestParams.get(0));
					boolean result = merge(otherPort);
					response = String.valueOf(result);
				}
				else if (requestType.equals("serverMerge")) {
					InputStream requestIn = exchange.getRequestBody();
					Scanner scanner = new Scanner(requestIn).useDelimiter("\\A");
					String result = scanner.hasNext() ? scanner.next() : "";
					Set<String> otherCausalHistory = parseCausalHistory(result);
					Set<String> diff = new HashSet<>(causalHistory);
					diff.removeAll(otherCausalHistory);
					response = diff.toString();
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
