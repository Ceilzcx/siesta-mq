package com.ceilzcx.siestamq.client.hook;

/**
 * @author ceilzcx
 * @since 27/9/2023
 *
 * 发送消息的hook, 相对来说感觉RpcClientHook用处不大
 */
public interface SendMessageHook {

    /**
     * 没有业务上的作用, 目前单纯只是打日志的时候使用
     * @return hook name
     */
    String hookName();

    void sendMessageBefore(final SendMessageContext context);

    void sendMessageAfter(final SendMessageContext context);

}
