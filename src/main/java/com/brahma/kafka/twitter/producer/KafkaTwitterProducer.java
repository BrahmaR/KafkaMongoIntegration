package com.brahma.kafka.twitter.producer;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.brahma.kafka.twitter.utils.ContextLoader;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

/**
 * Created by brahma on 22/12/16.
 */
public class KafkaTwitterProducer {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(KafkaTwitterProducer.class);
	private static final String BROKER_LIST = "kafka.broker.list";
	private static final String CONSUMER_KEY = "consumerKey";
	private static final String CONSUMER_SECRET = "consumerSecret";
	private static final String ACCESS_TOKEN = "accessToken";
	private static final String ACCESS_TOKEN_SECRET = "accessTokenSecret";
	private static final String KAFKA_TOPIC = "kafka.twitter.raw.topic";

	public static void runProducer(ContextLoader context) throws InterruptedException {
		// Producer properties
		Properties properties = new Properties();
		properties.put("metadata.broker.list", context.getString(BROKER_LIST));
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("request.required.acks", "1");
		ProducerConfig producerConfig = new ProducerConfig(properties);

		final Producer<String, String> producer = new Producer<String, String>(
				producerConfig);

		// Create an appropriately sized blocking queue
		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

		// create endpoint
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitterapi", "#love"));

		Authentication auth = new OAuth1(context.getString(CONSUMER_KEY),
				context.getString(CONSUMER_SECRET),
				context.getString(ACCESS_TOKEN),
				context.getString(ACCESS_TOKEN_SECRET));
		// create client
		BasicClient client = new ClientBuilder()
				// .name("twitterClient")
				.hosts(Constants.STREAM_HOST).endpoint(endpoint)
				.authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			if (client.isDone()) {
				LOGGER.info("Client connection closed unexpectedly: "
						+ client.getExitEvent().getMessage());
				break;
			}

			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(
						context.getString(KAFKA_TOPIC), queue.take());
				LOGGER.info(message.toString());
				System.out.println(message);
			} catch (InterruptedException ie) {
				LOGGER.error(ie.getMessage());
			}
			// send the message to the producer
			producer.send(message);
		}

		producer.close();
		client.stop();

		// Print some stats
		LOGGER.info("The client read %d messages!\n", client.getStatsTracker()
				.getNumMessages());
	}

	public static void main(String[] args) {
		try {
			ContextLoader context = new ContextLoader("config.properties");
			KafkaTwitterProducer.runProducer(context);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
}
