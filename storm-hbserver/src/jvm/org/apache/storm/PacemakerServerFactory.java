
package org.apache.storm;

import org.jboss.netty.handler.codec.serialization.*;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.AbstractCodec;
import com.twitter.finagle.Service;
import java.net.InetSocketAddress;

public class PacemakerServerFactory {

    private static AbstractCodec makeCodec() {
        
        return new AbstractCodec() {
            public ChannelPipelineFactory pipelineFactory() {
                return new ChannelPipelineFactory() {
                    public ChannelPipeline getPipeline() {
                        ChannelPipeline pipeline = Channels.pipeline();
                        pipeline.addLast("encoder", new ObjectEncoder());
                        pipeline.addLast("decoder", new ObjectDecoder());
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
