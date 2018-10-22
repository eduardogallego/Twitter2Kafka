package edu.bigdata.twitter2kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

class ConfigSt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigSt.class.getName());
    private static final Properties PROPERTIES = new Properties();
    private static final String CONFIG_FILE = "config.properties";
    private static ConfigSt instance;

    private ConfigSt() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (inputStream == null) {
                throw new Exception("Null Config InputStream");
            }
            PROPERTIES.load(inputStream);
        } catch (Exception ex) {
            LOGGER.error("Error reading Config File: " + ex.getMessage(), ex);
        }
    }

    static synchronized ConfigSt getInstance() {
        if (instance == null) {
            instance = new ConfigSt();
        }
        return instance;
    }

    String getConfig(String key) {
        return PROPERTIES.getProperty(key);
    }
}
