package com.ceilzcx.siestamq.remoting.protocol;

import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;

/**
 * @author ceilzcx
 * @since 13/12/2022
 */
public interface FastCodesHeader {

    default String getAndCheckNotNull(HashMap<String, String> fields, String field) {
        String value = fields.get(field);
        if (value == null) {
            String headerClass = this.getClass().getSimpleName();
//            RemotingCommand.log.error("the custom field {}.{} is null", headerClass, field);
            // no exception throws, keep compatible with RemotingCommand.decodeCommandCustomHeader
        }
        return value;
    }

    default void writeIfNotNull(ByteBuf out, String key, Object value) {
        if (value != null) {
            RocketMQSerializable.writeStr(out, true, key);
            RocketMQSerializable.writeStr(out, false, value.toString());
        }
    }

    void encode(ByteBuf out);

    void decode(HashMap<String, String> fields) throws RemotingCommandException;


}
