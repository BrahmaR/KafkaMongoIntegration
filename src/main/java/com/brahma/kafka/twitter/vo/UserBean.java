package com.brahma.kafka.twitter.vo;

import org.bson.Document;

public class UserBean {
	
	private String name;
	private String id_str;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId_str() {
		return id_str;
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}
	@Override
	public String toString() {
		return "User [name=" + name + ", id_str=" + id_str + "]";
	}
	
	public Document getUserAsDocument() {
        Document userDocument = new Document(
        		"name", getName())
                .append("id_str", getId_str());
                
        return userDocument;
    };
}
