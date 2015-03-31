
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
        @Override
        protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) {
            return ChannelBuffers.wrappedBuffer(
                Utils.thriftSerialize((TBase)msg)
                );
        }
    }

    private static class ThriftDecoder extends ReplayingDecoder {
        private static final Logger LOG = LoggerFactory.getLogger(ThriftDecoder.class);
        @Override
        protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buff, java.lang.Enum voidEnum) throws Exception {
            try {
                LOG.info("Have {} bytes.", buff.readableBytes());
                NettyTTransportAdapter adapter = new NettyTTransportAdapter(buff);
                TBinaryProtocol prot = new TBinaryProtocol(adapter);

                int index = buff.readerIndex();
                buff.markReaderIndex();

                LOG.info("Got index {}.", index);
                
                prot.readMessageBegin();
                TProtocolUtil.skip(prot, TType.STRUCT);
                prot.readMessageEnd();
                
                int endIndex = buff.readerIndex();
                buff.resetReaderIndex();

                LOG.info("Got End index {}.", endIndex);
                
                return Utils.thriftDeserialize(Message.class, buff.readSlice(endIndex - index).array());
            } catch (Throwable t) {
                LOG.info("Decoder Caught: {}", t);
                throw t;
            }
//            buff.markReaderIndex();
//            try {
//                return Utils.thriftDeserialize(Message.class, buff.array());
//            } catch (RuntimeException e) {
//                buff.resetReaderIndex();
//                return null;
//            }
        }

        protected Object decodeLast(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer, java.lang.Enum voidEnum) throws Exception {
            try {
                return decode(ctx, channel, buffer, voidEnum);
            } catch (Throwable t) {
                return null; // return null to indicate that not all expected bytes have been received yet
            }
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
