package com.ceilzcx.siesta.nameserver.processor;

import com.ceilzcx.siesta.nameserver.NameserverController;
import com.ceilzcx.siestamq.remoting.CommandCustomHeader;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import com.ceilzcx.siestamq.remoting.netty.NettyRequestProcessor;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.body.RegisterBrokerBody;
import com.ceilzcx.siestamq.remoting.protocol.body.TopicConfigSerializeWrapper;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.CreateTopicRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.nameserver.RegisterBrokerRequestHeader;
import com.ceilzcx.siestamq.remoting.protocol.header.nameserver.RegisterBrokerResponseHeader;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @author ceilzcx
 * @since 15/12/2022
 */
@Slf4j
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private final NameserverController nameserverController;

    public DefaultRequestProcessor(NameserverController nameserverController) {
        this.nameserverController = nameserverController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            case RequestCode.REGISTER_TOPIC_IN_NAMESERVER:
                return this.registerTopicInNameserver(ctx, request);
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request) {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        try {
            CreateTopicRequestHeader requestHeader = (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
            log.info("update or create topic, topic name: {}", requestHeader.getTopic());
            response.setCode(RemotingSysResponseCode.SUCCESS);
            return response;
        } catch (RemotingCommandException e) {
            e.printStackTrace();
        }
        return response;
    }

    private RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException, IOException {
        RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);

        RegisterBrokerRequestHeader requestHeader = (RegisterBrokerRequestHeader) request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
        RegisterBrokerBody requestBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed());

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = requestBody.getTopicConfigSerializeWrapper();



        return response;
    }

    private RemotingCommand registerTopicInNameserver(ChannelHandlerContext ctx, RemotingCommand request) {

        return null;
    }
}
