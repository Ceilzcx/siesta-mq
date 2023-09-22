package com.ceilzcx.siestamq.common.message;

import com.ceilzcx.siestamq.common.MixAll;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author ceilzcx
 * @since 22/9/2023
 * 批量消息发送, 继承Message感觉有点别扭, 估计是为了sendMessage入参复用使用, 后面可能会统一使用这个作为发送的消息的实体类
 */
public class MessageBatch extends Message implements Iterable<Message> {

    private final List<Message> messages;

    public MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    public static MessageBatch generateFromList(Collection<? extends Message> messages) {
        assert messages != null;
        assert messages.size() != 0;
        List<Message> messageList = new ArrayList<>(messages.size());
        Message first = null;
        for (Message message : messages) {
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
            }
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            if (first == null) {
                first = message;
            } else {
                if (!first.getTopic().equals(message.getTopic())) {
                    throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                }
                // todo isWaitStoreMsgOK
//                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
//                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
//                }
            }
            messageList.add(message);
        }
        MessageBatch messageBatch = new MessageBatch(messageList);
        messageBatch.setTopic(first.getTopic());
//        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());

        return messageBatch;
    }
}
