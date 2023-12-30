package com.github.quangtn.kafka.streams;

/*
* Load configuration from a file. This is mainly intended for connection info,
* so I can switch between clusters without recompile
* But you can put other client configuration here, but we may override it...
* */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class LoadConfigs {
    // default to cloud, duh

    private static final String DEFAULT_CONFIG_File =
            System.getProperty("user.home") + File.separator + ".ccloud" + File.separator + "config";

    static Properties loadConfig() throws IOException {
        return loadConfig(DEFAULT_CONFIG_File);
    }

    static Properties loadConfig(String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new RuntimeException(configFile + " does not exist. You need " +
                    "a file with client configuration");
        }
        System.out.println("Loading configs from: " + configFile);
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }

        return cfg;
    }
}
