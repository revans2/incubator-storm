/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.messaging.netty;

import java.io.IOException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send and receive SASL tokens.
 */

public class SaslMessageToken implements INettySerializable {
    public static final short IDENTIFIER = -500;

    /** Class logger */
    private static final Logger LOG = LoggerFactory
            .getLogger(SaslMessageToken.class);

    /** Used for client or server's token to send or receive from each other. */
    private byte[] token;

    /**
     * Constructor used for reflection only.
     */
    public SaslMessageToken() {
    }

    /**
     * Constructor used to send request.
     * 
     * @param token
     *            the SASL token, generated by a SaslClient or SaslServer.
     */
    public SaslMessageToken(byte[] token) {
        this.token = token;
    }

    /**
     * Read accessor for SASL token
     * 
     * @return saslToken SASL token
     */
    public byte[] getSaslToken() {
        return token;
    }

    /**
     * Write accessor for SASL token
     * 
     * @param token
     *            SASL token
     */
    public void setSaslToken(byte[] token) {
        this.token = token;
    }


    public int encodeLength() {
        return 2 + 4 + token.length;
    }

    /**
     * encode the current SaslToken Message into a channel buffer
     * SaslTokenMessageRequest is encoded as: identifier .... short(2) always it
     * is -500 payload length .... int payload .... byte[]
     * 
     * @throws Exception
     */
    public ChannelBuffer buffer() throws IOException {
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(
                ChannelBuffers.directBuffer(encodeLength()));
        int payload_len = 0;
        if (token != null)
            payload_len = token.length;


        bout.writeShort(IDENTIFIER);
        bout.writeInt((int) payload_len);
        if (payload_len > 0) {
            bout.write(token);
        }
        bout.close();
        return bout.buffer();
    }
    
    public static SaslMessageToken read(byte[] serial) {
        ChannelBuffer sm_buffer = ChannelBuffers.copiedBuffer(serial);
        short identifier = sm_buffer.readShort();
        int payload_len = sm_buffer.readInt();
        if(identifier != -500) {
            return null;
        }
        byte token[] = new byte[payload_len];
        sm_buffer.readBytes(token, 0, payload_len);
        return new SaslMessageToken(token);
    }
}
