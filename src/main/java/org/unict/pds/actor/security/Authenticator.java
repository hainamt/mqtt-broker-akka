package org.unict.pds.actor.security;

import akka.actor.AbstractActor;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import org.unict.pds.configuration.ConfigurationExtension;
import org.unict.pds.configuration.SecurityConfiguration;
import org.unict.pds.logging.LoggingUtils;
import org.unict.pds.message.security.ConnectWithAuthentication;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;


public class Authenticator extends AbstractActor {
    private SecurityConfiguration securityConfig;
    private Connection dbConnection;
    private String usersTable;
    private String rolesTable;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ConnectWithAuthentication.Request.class, this::handleConnectWithAuthentication)
                .build();
    }

    private void handleConnectWithAuthentication(ConnectWithAuthentication.Request request) {
        MqttConnectMessage message = request.message();
        String username = message.payload().userName();
        String password = message.payload().passwordInBytes() != null
                ? new String(message.payload().passwordInBytes())
                : null;

        if (username == null || password == null) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.INFO,
                    "Authentication failed: Missing credentials",
                    "Authenticator"
            );

            sender().tell(new ConnectWithAuthentication.Response(
                    false,
                    username,
                    null
            ), self());
            return;
        }
        try {
            if (authenticate(username, password)) {
                ConnectWithAuthentication.Role role = securityConfig.authorizationEnabled()
                        ? getUserRole(username)
                        : new ConnectWithAuthentication.Role(new HashSet<>(), new HashSet<>());

                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.INFO,
                        "Authentication successful for user: " + username,
                        "Authenticator"
                );

                sender().tell(new ConnectWithAuthentication.Response(true, username, role), self());
            } else {
                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.INFO,
                        "Authentication failed for user: " + username,
                        "Authenticator"
                );

                sender().tell(new ConnectWithAuthentication.Response(false, username, null), self());
            }
        } catch (SQLException e) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "Database error during authentication: " + e.getMessage(),
                    "Authenticator"
            );

            sender().tell(new ConnectWithAuthentication.Response(false, username, null), self());
        }
    }

    @Override
    public void preStart() {
        this.securityConfig = ConfigurationExtension.getInstance()
                .get(getContext().getSystem()).securityConfig();

        this.usersTable = createQuotedTableName(securityConfig.usersTable());
        this.rolesTable = createQuotedTableName(securityConfig.rolesTable());

        try {
            dbConnection = DriverManager.getConnection(
                    securityConfig.jdbcUrl(),
                    securityConfig.jdbcUsername(),
                    securityConfig.jdbcPassword()
            );
        } catch (SQLException e) {
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    "Failed to connect to authentication database: " + e.getMessage(),
                    "Authenticator"
            );
            throw new RuntimeException(e);
        }

        LoggingUtils.logApplicationEvent(
                LoggingUtils.LogLevel.INFO,
                "Authenticator started, connected to database " + securityConfig.jdbcUrl(),
                "Authenticator"
        );
    }

    @Override
    public void postStop() {
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException e) {
                LoggingUtils.logApplicationEvent(
                        LoggingUtils.LogLevel.ERROR,
                        "Error closing database connection: " + e.getMessage(),
                        "Authenticator"
                );
            }
        }
    }

    private boolean authenticate(String username, String password) throws SQLException {
        String sql = "SELECT password_hash FROM " + usersTable + " WHERE username = ?";
        try (PreparedStatement statement = dbConnection.prepareStatement(sql)) {
            statement.setString(1, username);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    String storedPasswordHash = resultSet.getString("password_hash");
                    return password.equals(storedPasswordHash);
                }
            }
        }
        return false;
    }



    private ConnectWithAuthentication.Role getUserRole(String username) throws SQLException {
        Set<String> pubAllowedTopics = new HashSet<>();
        Set<String> subAllowedTopics = new HashSet<>();

        String sql = "SELECT t.topic_name, p.can_publish, p.can_subscribe " +
                "FROM " + rolesTable + " p " +
                "JOIN topics t ON p.topic_id = t.id " +
                "JOIN " + usersTable + " u ON p.user_id = u.id " +
                "WHERE u.username = ?";

        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            stmt.setString(1, username);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String topicName = rs.getString("topic_name");
                    boolean canPublish = rs.getBoolean("can_publish");
                    boolean canSubscribe = rs.getBoolean("can_subscribe");

                    if (canPublish) {
                        pubAllowedTopics.add(topicName);
                    }
                    if (canSubscribe) {
                        subAllowedTopics.add(topicName);
                    }
                }
            }
        }
        return new ConnectWithAuthentication.Role(pubAllowedTopics, subAllowedTopics);
    }


    private String createQuotedTableName(String fullTableName) {
        String[] parts = fullTableName.split("\\.", 2);
        if (parts.length == 2) {
            // Schema and table provided
            return "\"" + parts[0] + "\".\"" + parts[1] + "\"";
        } else {
            // Only table name provided
            return "\"" + fullTableName + "\"";
        }
    }

}
