import java.time.LocalDateTime;
import java.util.List;
import java.util.ArrayList;

public class Triple {
	private String subject;
	private String predicate;
	private String object;
	private String requestId;
	private LocalDateTime timeStamp;

	public Triple(String subject, String predicate, String object, String requestId, LocalDateTime timeStamp) {
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.requestId = requestId;
		this.timeStamp = timeStamp;
	}

	public String getSubject() {
		return subject;
	}

	public String getPredicate() {
		return predicate;
	}

	public String getObject() {
		return object;
	}
	public String getRequestId() {
		return requestId;
	}
	public LocalDateTime getTimeStamp() {
		return timeStamp;
	}

	public String toString() {
		String result = "{" + subject + "," + predicate + "," + object + "," + requestId + "," + timeStamp.toString() + "}";
		return result;
	}	       
	
	public static String triplesToString(List<Triple> triples) {
		String result = "";
		for (Triple triple: triples) {
			result += triple.toString() + "|";
		}
		result = result.substring(0, result.length()-1);
		return result;
	}
	
	public static List<Triple> stringToTriples(String input) {
        List<Triple> triples = new ArrayList<>();
        String[] parts = input.split("\\|");
        for (String part : parts) {
            part = part.substring(1, part.length() - 1);
            String[] values = part.split(",");
            String subject = values[0];
            String predicate = values[1];
            String object = values[2];
            String requestId = values[3];
            LocalDateTime timeStamp = LocalDateTime.parse(values[4]);
            Triple triple = new Triple(subject, predicate, object, requestId, timeStamp);
            triples.add(triple);
        }
        return triples;
    }
	
}
