package TriDaS;

import java.util.List;

public class Request {
	private String id;
	private String type;
	private List<String> params;
	
	public Request(String id, String type, List<String> params) {
		this.id = id;
		this.type = type;
		this.params = params;
	}

	public String getId() {
		return id;
	}
		
	public String getType() {
		return type;
	}

	public List<String> getParams() {
		return params;
	}
}
