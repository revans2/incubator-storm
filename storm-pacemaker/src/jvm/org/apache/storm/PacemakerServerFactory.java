
package org.apache.storm;


import org.jboss.netty.handler.codec.serialization.*;
import org.jboss.netty.handler.codec.replay.ReplayingDecoder;
import org.jboss.netty.handler.codec.replay.VoidEnum;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.AbstractCodec;
import com.twitter.finagle.Service;
import java.net.InetSocketAddress;
import backtype.storm.utils.Utils;
import backtype.storm.generated.Message;
import org.apache.thrift7.TBase;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TType;
import org.apache.thrift7.protocol.TProtocolUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PacemakerServerFactory {

    private static class ThriftEncoder extends OneToOneEncoder {
        private static final Logger LOG = LoggerFactory.getLogger(ThriftDecoder.class);
        
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

    private static class ThriftDecoder extends FrameDecoder {

        private static final Logger LOG = LoggerFactory.getLogger(ThriftDecoder.class);
        
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
    
    private static AbstractCodec makeCodec() {
        
        return new AbstractCodec() {
            public ChannelPipelineFactory pipelineFactory() {
                return new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() {
                        ChannelPipeline pipeline = Channels.pipeline();
                        pipeline.addLast("encoder", new ThriftEncoder());
                        pipeline.addLast("decoder", new ThriftDecoder());
                        return pipeline;
                    }
                };
            }
        };
    }
    
    public static Server makeServer(String name, int port, Service service) {

        return ServerBuilder.safeBuild(
            service,
            ServerBuilder.get()
            .name(name)
            .codec(makeCodec())
            .bindTo(new InetSocketAddress(port)));
    }

    public static Service makeClient(String host) {
        return ClientBuilder.safeBuild(
            ClientBuilder.get()
            .codec(makeCodec())
            .hosts(host)
            .hostConnectionLimit(1)
            );
            
    }
}
