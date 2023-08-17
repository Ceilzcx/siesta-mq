package com.ceilzcx.siestamq.broker.processor;

import com.ceilzcx.siestamq.broker.BrokerController;
import com.ceilzcx.siestamq.common.TopicConfig;
import com.ceilzcx.siestamq.common.topic.TopicValidator;
import com.ceilzcx.siestamq.remoting.common.RemotingHelper;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import com.ceilzcx.siestamq.remoting.netty.NettyRequestProcessor;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.ceilzcx.siestamq.remoting.protocol.RequestCode;
import com.ceilzcx.siestamq.remoting.protocol.header.broker.CreateTopicRequestHeader;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @author ceilzcx
 * @since 19/12/2022
 */
@Slf4j
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private final BrokerController brokerController;

    public AdminBrokerProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    private RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        CreateTopicRequestHeader requestHeader = (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);

        log.info("Broker receive request to update or create topic={}, caller address={}",
                requestHeader.getTopic(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        TopicValidator.ValidateTopicResult result = TopicValidator.validateTopic(requestHeader.getTopic());
        if (!result.isValid()) {
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark(result.getRemark());
            return response;
        }

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setPerm(requestHeader.getPerm());

        try {
            this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);
        } catch (Exception e) {
            log.error("Update / create topic failed for [{}]", request, e);
            response.setCode(RemotingSysResponseCode.SYSTEM_ERROR);
            response.setRemark(e.getMessage());
        }
        return response;
    }

}
