
package org.apache.storm;

import org.jboss.netty.buffer.ChannelBuffer;
import org.apache.thrift7.transport.TTransport;


public class NettyTTransportAdapter extends TTransport {

    private ChannelBuffer in;

    public NettyTTransportAdapter(ChannelBuffer input) {
        in = input;
    }

    public void close() {}

    public boolean isOpen() { return true; }

    public void open() {}

    public int read(byte[] buf, int off, int len) {
        int toRead = len;
        if(len > in.readableBytes()) {
            toRead = in.readableBytes();
        }

        in.readBytes(buf, off, toRead);
        return toRead;
    }

    public void write(byte[] buf, int off, int len) {
        
    }
    
}

