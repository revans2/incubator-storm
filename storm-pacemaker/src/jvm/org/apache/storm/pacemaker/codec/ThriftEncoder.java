package org.apache.storm.pacemaker.codec;

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import backtype.storm.generated.Message;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import backtype.storm.utils.Utils;

public class ThriftEncoder extends OneToOneEncoder {

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) {

        byte serialized[] = Utils.thriftSerialize((Message)msg);
        Message m = (Message)Utils.thriftDeserialize(Message.class, serialized);
        ChannelBuffer ret = ChannelBuffers.directBuffer(serialized.length + 4);


        ret.writeInt(serialized.length);
        ret.writeBytes(serialized);

        return ret;
    }
}
