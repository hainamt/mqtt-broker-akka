package org.unict.pds.actor;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import org.unict.pds.exception.CriticalActorCouldNotBeResolved;
import org.unict.pds.logging.LoggingUtils;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ActorResolutionUtils {

    public static ActorRef resolveActor(
            ActorSystem system, 
            String actorPath, 
            Duration timeout, 
            String componentName,
            boolean isCritical) {
        
        if (actorPath == null || actorPath.isEmpty()) {
            String errorMsg = componentName + ": Actor path is null or empty";
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    errorMsg,
                    "ActorResolutionUtils"
            );
            if (isCritical) {
                throw new RuntimeException(errorMsg);
            }
            return null;
        }
        
        ActorSelection selection = system.actorSelection(actorPath);
        CompletionStage<ActorRef> future = selection.resolveOne(timeout);
        
        try {
            return future.toCompletableFuture().get(timeout.getSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            String errorMsg = componentName + " could not resolve actor at path: " + actorPath;
            LoggingUtils.logApplicationEvent(
                    LoggingUtils.LogLevel.ERROR,
                    errorMsg + ": " + e.getMessage(),
                    "ActorResolutionUtils"
            );
            
            if (isCritical) {
                throw new CriticalActorCouldNotBeResolved(errorMsg, e);
            }
            return null;
        }
    }
}