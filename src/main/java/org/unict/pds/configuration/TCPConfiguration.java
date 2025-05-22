package org.unict.pds.configuration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


public record TCPConfiguration(String hostname, int port, int backlog) {
    public static TCPConfiguration load() {
        Config config = ConfigFactory.load().getConfig("tcp-server");
        return new TCPConfiguration(
                config.getString("host"),
                config.getInt("port"),
                config.getInt("backlog")
        );
    }
}
