package com.brahma.kafka.twitter.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by brahma.
 */
public class ContextLoader {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ContextLoader.class);

	Properties properties;

	public ContextLoader(String configFileLocation) throws Exception {
		properties = new Properties();
		try {
			InputStream is = getClass().getClassLoader().getResourceAsStream(
					configFileLocation);
			properties.load(is);
		} catch (FileNotFoundException e) {
			LOGGER.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		} catch (IOException e) {
			LOGGER.error("Encountered error while reading configuration properties: "
					+ e.getMessage());
			throw e;
		}
	}

	public String getString(String key) {
		return properties.getProperty(key);
	}
}