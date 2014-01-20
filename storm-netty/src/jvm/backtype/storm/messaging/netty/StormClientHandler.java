package backtype.storm.messaging.netty;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormClientHandler extends SimpleChannelUpstreamHandler  {
    private static final Logger LOG = LoggerFactory.getLogger(StormClientHandler.class);
    private Client client;
    long start_time;
    
    StormClientHandler(Client client) {
        this.client = client;
        start_time = System.currentTimeMillis();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent event) {
        //register the newly established channel
        Channel channel = event.getChannel();
        client.setChannel(channel);
        LOG.debug("connection established from "+channel.getLocalAddress()+" to "+channel.getRemoteAddress());
        
        //send next batch of requests if any
        try {
            client.tryDeliverMessages();
        } catch (Exception ex) {
            LOG.info("exception when sending messages:", ex.getMessage());
            client.reconnect();
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent event) {
        LOG.debug("send/recv time (ms): {}", (System.currentTimeMillis() - start_time));
        
        //examine the response message from server
        ControlMessage msg = (ControlMessage)event.getMessage();
        if (msg==ControlMessage.FAILURE_RESPONSE)
            LOG.info("failure response:{}", msg);

        //send next batch of requests if any
        try {
            client.tryDeliverMessages();
        } catch (Exception ex) {
            LOG.info("exception when sending messages:", ex.getMessage());
            client.reconnect();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent event) {
        Throwable cause = event.getCause();
        if (!(cause instanceof ConnectException)) {
            LOG.info("Connection failed:", cause);
        }
        client.reconnect();
    }
}
