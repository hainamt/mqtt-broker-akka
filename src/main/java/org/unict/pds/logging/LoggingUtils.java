package org.unict.pds.logging;

import io.netty.handler.codec.mqtt.MqttMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;

public class LoggingUtils {

    private static final Logger PROTOCOL_LOGGER = LoggerFactory.getLogger("protocol");
    private static final Logger INTERNAL_LOGGER = LoggerFactory.getLogger("internal");
    private static final Logger APP_LOGGER = LoggerFactory.getLogger("application");

    public enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR
    }

    public static void logProtocolMessage(LogLevel level,
                                         MqttMessageType messageType,
                                         String source,
                                         String destination,
                                         boolean isInbound) {
        String callerClass = getCallerClassName();
        MDC.put("messageType", messageType.toString());
        MDC.put("timestamp", Instant.now().toString());
        MDC.put("source", source);
        MDC.put("destination", destination);
        MDC.put("direction", isInbound ? "INBOUND" : "OUTBOUND");
        MDC.put("level", level.toString());
        MDC.put("callerClass", callerClass);

        String message = "MQTT {} message from {} to {} ({})";
        
        switch (level) {
            case TRACE:
                PROTOCOL_LOGGER.trace(message, messageType, source, destination, callerClass);
                break;
            case DEBUG:
                PROTOCOL_LOGGER.debug(message, messageType, source, destination, callerClass);
                break;
            case INFO:
                PROTOCOL_LOGGER.info(message, messageType, source, destination, callerClass);
                break;
            case WARN:
                PROTOCOL_LOGGER.warn(message, messageType, source, destination, callerClass);
                break;
            case ERROR:
                PROTOCOL_LOGGER.error(message, messageType, source, destination, callerClass);
                break;
        }

        MDC.clear();
    }

    public static void logInternalMessage(LogLevel level, 
                                         Class<?> messageClass, 
                                         String sourceActor, 
                                         String destinationActor,
                                          String message) {
        String callerClass = getCallerClassName();
        MDC.put("messageClass", messageClass.getSimpleName());
        MDC.put("timestamp", Instant.now().toString());
        MDC.put("sourceActor", sourceActor);
        MDC.put("destinationActor", destinationActor);
        MDC.put("message", message);
        MDC.put("type", "INTERNAL");
        MDC.put("level", level.toString());
        MDC.put("callerClass", callerClass);
        
        String logMessage = message + " ({})";
        
        switch (level) {
            case TRACE:
                INTERNAL_LOGGER.trace(logMessage, callerClass);
                break;
            case DEBUG:
                INTERNAL_LOGGER.debug(logMessage, callerClass);
                break;
            case INFO:
                INTERNAL_LOGGER.info(logMessage, callerClass);
                break;
            case WARN:
                INTERNAL_LOGGER.warn(logMessage, callerClass);
                break;
            case ERROR:
                INTERNAL_LOGGER.error(logMessage, callerClass);
                break;
        }

        MDC.clear();
    }

    public static void logApplicationEvent(LogLevel level, String message, String component) {
        String callerClass = getCallerClassName();
        MDC.put("timestamp", Instant.now().toString());
        MDC.put("component", component);
        MDC.put("message", message);
        MDC.put("type", "APPLICATION");
        MDC.put("level", level.toString());
        MDC.put("callerClass", callerClass);

        String logMessage = "{} ({})";

        switch (level) {
            case TRACE:
                APP_LOGGER.trace(logMessage, message, callerClass);
                break;
            case DEBUG:
                APP_LOGGER.debug(logMessage, message, callerClass);
                break;
            case INFO:
                APP_LOGGER.info(logMessage, message, callerClass);
                break;
            case WARN:
                APP_LOGGER.warn(logMessage, message, callerClass);
                break;
            case ERROR:
                APP_LOGGER.error(logMessage, message, callerClass);
                break;
        }

        MDC.clear();
    }

    private static String getCallerClassName() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        if (stackTrace.length > 3) {
            String fullClassName = stackTrace[3].getClassName();
            int lastDot = fullClassName.lastIndexOf('.');
            return lastDot > 0 ? fullClassName.substring(lastDot + 1) : fullClassName;
        }
        return "Unknown";
    }
}