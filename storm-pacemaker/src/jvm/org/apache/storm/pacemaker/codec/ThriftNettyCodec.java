package org.apache.storm.pacemaker.codec;

import com.twitter.finagle.AbstractCodec;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;

public class ThriftNettyCodec extends AbstractCodec{

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
}
