package org.unict.pds.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.unict.pds.logging.LoggingUtils;

public record SecurityConfiguration(
        boolean authenticationEnabled,
        boolean authorizationEnabled,
        String jdbcUrl,
        String jdbcUsername,
        String jdbcPassword,
        String usersTable,
        String rolesTable
) implements LoadableActorConfig {
    public static SecurityConfiguration load() {
        Config config = ConfigFactory.load().getConfig("security");
        boolean authenticationEnabled = config.getBoolean("authentication-enabled");
        boolean authorizationEnabled = config.getBoolean("authorization-enabled");
        if (!authenticationEnabled && authorizationEnabled) {
            authorizationEnabled = false;
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.WARN,
                    "No authorization will be performed when Authenticator is disabled",
                    "SecurityConfiguration"
            );
        }
        return new SecurityConfiguration(
                authenticationEnabled,
                authorizationEnabled,
                config.getString("jdbc-url"),
                config.getString("jdbc-username"),
                config.getString("jdbc-password"),
                config.getString("users-table"),
                config.getString("roles-table")
        );
    }
}
