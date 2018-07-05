package com.brahma.kafka.twitter.vo;

import org.bson.Document;

public class TwitterHashTagBean {

	private String created_at;
	private String id_str;
	private String text;
	private String source;

	/* private User user; */

	public TwitterHashTagBean(String created_at, String id_str, /* User user, */
			String text, String source) {
		super();
		this.created_at = created_at;
		this.id_str = id_str;
		/* this.user = user; */
		this.text = text;
		this.source = source;
	}

	@Override
	public String toString() {
		return "TwitterHashTagBean [created_at=" + created_at + ", id_str="
				+ id_str + ", text=" + text + ", source=" + source + "]";
	}

	/*
	 * public User getUser() { return user; } public void setUser(User user) {
	 * this.user = user; }
	 */
	public String getCreated_at() {
		return created_at;
	}

	public void setCreated_at(String created_at) {
		this.created_at = created_at;
	}

	public String getId_str() {
		return id_str;
	}

	public void setId_str(String id_str) {
		this.id_str = id_str;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public Document getTweetAsDocument() {
		Document twitterHashTagDocument = new Document("created_at",
				getCreated_at()).append("id_str", getId_str())
		/* .append("user", getUser()) */
		.append("text", getText()).append("source", getSource());
		return twitterHashTagDocument;
	};
}
