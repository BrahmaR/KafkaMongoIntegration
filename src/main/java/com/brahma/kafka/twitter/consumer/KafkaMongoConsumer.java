package com.brahma.kafka.twitter.consumer;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.bson.Document;

import com.brahma.kafka.twitter.utils.ContextLoader;
import com.brahma.kafka.twitter.vo.TwitterHashTagBean;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * Created by brahma on 22/12/16.
 */
public class KafkaMongoConsumer {

	private static final String BROKER_LIST = "kafka.broker.list";
	private static final String HASHTAG_COLLECTION = "hashtag.collection";
	private static final String DB_NAME = "db.name";
	static ContextLoader context = null;

	public static void main(String args[]) {
		KafkaMongoConsumer example = new KafkaMongoConsumer();
		// long maxReads = Long.parseLong(args[0]);
		long maxReads = 1000;
		// String topic = args[1];
		String topic = "kafka.twitter.raw.topic";
		// int partition = Integer.parseInt(args[2]);
		int partition = 0;

		// seeds.add(args[3]);
		List<String> seeds = null;
		int port = 0;
		try {
			context = new ContextLoader("config.properties");
			seeds = new ArrayList<String>();
			String host = context.getString(BROKER_LIST);
			seeds.add(host.substring(0, host.indexOf(":")));
			// int port = Integer.parseInt(args[4]);
			port = Integer.parseInt(host.substring(host.indexOf(":") + 1,
					host.length()));
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			example.run(maxReads, context.getString(topic), partition, seeds,
					port);
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}

	private List<String> m_replicaBrokers = new ArrayList<String>();

	public KafkaMongoConsumer() {
		m_replicaBrokers = new ArrayList<String>();
	}

	public void run(long a_maxReads, String a_topic, int a_partition,
			List<String> a_seedBrokers, int a_port) throws Exception {
		// find the meta data about the topic and partition we are interested in
		//
		PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic,
				a_partition);
		if (metadata == null) {
			System.out
					.println("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			System.out
					.println("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + a_topic + "_" + a_partition;

		
		/* We can replace the below part with the Kafka streams */
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port,
				100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, a_topic, a_partition,
				kafka.api.OffsetRequest.EarliestTime(), clientName);

		int numErrors = 0;
		while (a_maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, a_port, 100000,
						64 * 1024, clientName);
			}
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(a_topic, a_partition, readOffset, 100000).build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(a_topic, a_partition);
				System.out.println("Error fetching data from the Broker:"
						+ leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask
					// for the last element to reset
					readOffset = getLastOffset(consumer, a_topic, a_partition,
							kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, a_topic, a_partition,
						a_port);
				continue;
			}
			numErrors = 0;
			long numRead = 0;

			// Inserting data to the Mongo Db
			MongoClient client = new MongoClient();
			MongoDatabase db = client.getDatabase(context.getString(DB_NAME));
			MongoCollection<Document> tweetCollection = db
					.getCollection(context.getString(HASHTAG_COLLECTION));
			Gson gson = new Gson();
			Type type = new TypeToken<TwitterHashTagBean>() {
			}.getType();
			// Type userType = new TypeToken<User>() {
			// }.getType();

			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(
					a_topic, a_partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					System.out.println("Found an old offset: " + currentOffset
							+ " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				// User user = gson.fromJson(new String(
				// bytes, "UTF-8"), userType);
				TwitterHashTagBean incomingTweet = gson.fromJson(new String(
						bytes, "UTF-8"), type);
				// incomingTweet.setUser(user);
				System.out.println(incomingTweet);
				tweetCollection.insertOne(incomingTweet.getTweetAsDocument());

				numRead++;
				a_maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		if (consumer != null)
			consumer.close();
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			System.out
					.println("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}

	private String findNewLeader(String a_oldLeader, String a_topic,
			int a_partition, int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port,
					a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper
				// a second to recover second time, assume the broker did
				// recover before failover,
				// or it was a non-Broker issue
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		System.out
				.println("Unable to find new leader after Broker failure. Exiting");
		throw new Exception(
				"Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> a_seedBrokers,
			int a_port, String a_topic, int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024,
						"leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", "
						+ a_partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			m_replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				m_replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
}
