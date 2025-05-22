package org.unict.pds.message.publish;

public record PublishManagerResponse(
        int messageId,
        boolean success
) {
}
