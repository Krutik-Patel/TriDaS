import java.time.LocalDateTime;

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
}
