package org.apache.storm.pacemaker.codec;

import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.buffer.ChannelBuffer;
import backtype.storm.generated.Message;
import backtype.storm.utils.Utils;

public class ThriftDecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {

        long available = buf.readableBytes();
        if(available < 2) {
            return null;
        }

        buf.markReaderIndex();

        int thriftLen = buf.readInt();
        available -= 4;


        if(available < thriftLen) {
            // We haven't received the entire object yet, return and wait for more bytes.
            buf.resetReaderIndex();
            return null;
        }

        buf.discardReadBytes();

        byte serialized[] = new byte[thriftLen];
        buf.readBytes(serialized, 0, thriftLen);

        Message m = (Message)Utils.thriftDeserialize(Message.class, serialized);

        return m;
    }
}
