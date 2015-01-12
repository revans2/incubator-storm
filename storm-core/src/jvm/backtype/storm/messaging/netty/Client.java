/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;

import backtype.storm.Config;
import backtype.storm.metric.api.IStatefulObject;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.Utils;
import backtype.storm.utils.StormBoundedExponentialBackoffRetry;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class Client implements IConnection, IStatefulObject{
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);
    private static final Timer TIMER = new Timer("netty-client-timer", true);

    private final int max_retries;
    private final int base_sleep_ms;
    private final int max_sleep_ms;
    private LinkedBlockingQueue<Object> message_queue; //entry should either be TaskMessage or ControlMessage
    private AtomicReference<Channel> channelRef;
    private final ClientBootstrap bootstrap;
    InetSocketAddress remote_addr;
    private AtomicInteger retries;
    private AtomicInteger totalReconnects;
    private AtomicInteger messagesEnqueued;
    private AtomicInteger messagesSent;
    private AtomicInteger messagesLostReconnect;
    private final Random random = new Random();
    private final ChannelFactory factory;
    private final int buffer_size;
    private final AtomicBoolean being_closed;
    private final AtomicBoolean close_msg_enqueued;
    private boolean wait_for_requests;
    private final StormBoundedExponentialBackoffRetry retryPolicy;

    @SuppressWarnings("rawtypes")
    Client(Map storm_conf, ChannelFactory factory, String host, int port) {
        this.factory = factory;
        message_queue = new LinkedBlockingQueue<Object>();
        retries = new AtomicInteger(0);
        channelRef = new AtomicReference<Channel>(null);
        being_closed = new AtomicBoolean(false);
        close_msg_enqueued = new AtomicBoolean(false);
        wait_for_requests = false;
        totalReconnects = new AtomicInteger(0);
        messagesSent = new AtomicInteger(0);
        messagesEnqueued = new AtomicInteger(0);
        messagesLostReconnect = new AtomicInteger(0);

        // Configure
        buffer_size = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_BUFFER_SIZE));
        max_retries = Math.min(30, Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_RETRIES)));
        base_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MIN_SLEEP_MS));
        max_sleep_ms = Utils.getInt(storm_conf.get(Config.STORM_MESSAGING_NETTY_MAX_SLEEP_MS));
        retryPolicy = new StormBoundedExponentialBackoffRetry(base_sleep_ms, max_sleep_ms, max_retries);

        bootstrap = new ClientBootstrap(factory);
        bootstrap.setOption("tcpNoDelay", true);
        bootstrap.setOption("sendBufferSize", buffer_size);
        bootstrap.setOption("keepAlive", true);

        // Set up the pipeline factory.
        bootstrap.setPipelineFactory(new StormClientPipelineFactory(this));

        // Start the connection attempt.
        remote_addr = new InetSocketAddress(host, port);
        bootstrap.connect(remote_addr);
    }

    /**
     * We will retry connection with exponential back-off policy
     */
    void reconnect() {
        close_n_release();

        //reconnect only if it's not being closed
        if (being_closed.get()) return;

        final int tried_count = retries.incrementAndGet();
        totalReconnects.incrementAndGet();
        long sleep = retryPolicy.getSleepTimeMs(retries.get(), 0);
        LOG.info("Waiting {} ms before trying connection to {}", sleep, remote_addr);
        TIMER.schedule(new TimerTask() {
            @Override
            public void run() { 
                if (being_closed.get()) {
                    LOG.debug(
                            "Not reconnecting to {} since we are closing [{}]",
                            remote_addr, tried_count);
                    return;
                }
                LOG.info("Reconnect ... [{}] to {}", tried_count, remote_addr);
                bootstrap.connect(remote_addr);
            }}, sleep);
    }

    /**
     * Enqueue a task message to be sent to server
     */
    @Override
    public void send(int task, byte[] message) {
        //throw exception if the client is being closed
        if (being_closed.get()) {
            throw new RuntimeException("Client is being closed, and does not take requests any more");
        }

        try {
            message_queue.put(new TaskMessage(task, message));
            messagesEnqueued.incrementAndGet();
            //resume delivery if it is waiting for requests
            tryDeliverMessages(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(Iterator<TaskMessage> msgs) {
        //throw exception if the client is being closed
        if (being_closed.get()) {
            throw new RuntimeException("Client is being closed, and does not take requests any more");
        }

        try {
            while(msgs.hasNext()) {
              message_queue.put(msgs.next());
              messagesEnqueued.incrementAndGet();
            }

            //resume delivery if it is waiting for requests
            tryDeliverMessages(true);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve messages from queue, and delivery to server if any
     */
    synchronized void tryDeliverMessages(boolean only_if_waiting) throws InterruptedException {
        //just skip if delivery only if waiting, and we are not waiting currently
        if (only_if_waiting && !wait_for_requests)  {
          //LOG.info("Cutting out early, not waiting for requests... ("+message_queue.size()+")");
          return;
        }

        //make sure that channel was not closed
        Channel channel = channelRef.get();
        if ((channel == null || !channel.isOpen())
                && close_msg_enqueued.get()) {
            LOG.debug("Channel: ({}), Close Message Enqueued: ({})", channel, close_msg_enqueued.get());
            being_closed.set(true);
            return;
        }
        if (channel == null) {
            return;
        }
        if (!channel.isOpen()) {
            LOG.info("Channel to {} is no longer open.",remote_addr);
            //The channel is not open yet. Reconnect?
            reconnect();
            return;
        }

        final MessageBatch requests = tryTakeMessages();
        if (requests==null) {
            wait_for_requests = true;
            return;
        }

        //if channel is being closed and we have no outstanding messages,  let's close the channel
        if (requests.isEmpty() && being_closed.get()) {
            close_n_release();
            return;
        }

        //we are busily delivering messages, and will check queue upon response.
        //When send() is called by senders, we should not thus call tryDeliverMessages().
        wait_for_requests = false;

        //write request into socket channel
        ChannelFuture future = channel.write(requests);
        future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future)
                    throws Exception {
                if (!future.isSuccess()) {
                    messagesLostReconnect.addAndGet(requests.size());
                    LOG.info("failed to send "+requests.size()+" requests to "+remote_addr, future.getCause());
                    reconnect();
                } else {
                    messagesSent.addAndGet(requests.size());
                    //LOG.info("{} request(s) sent", requests.size());

                    //Now that our requests have been sent, channel could be closed if needed
                    if (being_closed.get()) {
                        close_n_release();
                    } else {
                        tryDeliverMessages(false);
                    }
                }
            }
        });
    }

    /**
     * Take all enqueued messages from queue
     * @return  batch of messages
     * @throws InterruptedException
     *
     * synchronized ... ensure that messages are delivered in the same order
     * as they are added into queue
     */
    private MessageBatch tryTakeMessages() throws InterruptedException {
        //1st message
        Object msg = message_queue.poll();
        if (msg == null) return null;

        MessageBatch batch = new MessageBatch(buffer_size);
        //we will discard any message after CLOSE
        if (msg == ControlMessage.CLOSE_MESSAGE) {
            LOG.info("Connection to {} is being closed", remote_addr);
            being_closed.set(true);
            return batch;
        }

        batch.add((TaskMessage)msg);
        while (!batch.isFull() && ((msg = message_queue.peek())!=null)) {
            //Is it a CLOSE message?
            if (msg == ControlMessage.CLOSE_MESSAGE) {
                message_queue.take();
                LOG.info("Connection to {} is being closed", remote_addr);
                being_closed.set(true);
                break;
            }

            //try to add this msg into batch
            if (!batch.tryAdd((TaskMessage) msg))
                break;

            //remove this message
            message_queue.take();
        }

        return batch;
    }

    /**
     * gracefully close this client.
     *
     * We will send all existing requests, and then invoke close_n_release() method
     */
    public void close() {
        //enqueue a CLOSE message so that shutdown() will be invoked
        try {
            LOG.debug("Enqueing close message");
            message_queue.put(ControlMessage.CLOSE_MESSAGE);
            close_msg_enqueued.set(true);

            //resume delivery if it is waiting for requests
            tryDeliverMessages(true);
        } catch (InterruptedException e) {
            LOG.info("Interrupted Connection to {} is being closed", remote_addr);
            being_closed.set(true);
            close_n_release();
        }
    }

    /**
     * close_n_release() is invoked after all messages have been sent.
     */
    synchronized void close_n_release() {
        if (channelRef.get() != null) {
            channelRef.get().close();
            LOG.debug("channel {} closed",remote_addr);
            setChannel(null);
        }
    }

    @Override
    public Iterator<TaskMessage> recv(int flags, int clientId) {
        throw new RuntimeException("Client connection should not receive any messages");
    }

    void setChannel(Channel channel) {
        if (channel != null && channel.isOpen()) {
            //Assume the most recent connection attempt was successful.
            retries.set(0);
        }
        channelRef.set(channel);
        //reset retries
        if (channel != null)
            retries.set(0);
    }

    @Override
    public Object getState() {
        LOG.info("Getting metrics for connection to "+remote_addr);
        HashMap<String, Object> ret = new HashMap<String, Object>();
        ret.put("queue_length", message_queue.size());
        ret.put("reconnects", totalReconnects.getAndSet(0));
        ret.put("enqueued", messagesEnqueued.getAndSet(0));
        ret.put("sent", messagesSent.getAndSet(0));
        ret.put("lostOnSend", messagesLostReconnect.getAndSet(0));
        ret.put("dest", remote_addr.toString());
        Channel c = channelRef.get();
        if (c != null) {
            SocketAddress address = c.getLocalAddress();
            if (address != null) {
              ret.put("src", address.toString());
            }
        }
        return ret;
    }
}
