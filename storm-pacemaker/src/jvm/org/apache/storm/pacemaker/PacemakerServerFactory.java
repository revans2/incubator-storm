package org.apache.storm.pacemaker;

import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.Service;
import java.net.InetSocketAddress;
import org.apache.storm.pacemaker.codec.ThriftNettyCodec;

public class PacemakerServerFactory {

    public static Server makeServer(String name, int port, Service service) {

        return ServerBuilder.safeBuild(
            service,
            ServerBuilder.get()
            .name(name)
            .codec(new ThriftNettyCodec())
            .bindTo(new InetSocketAddress(port)));
    }

    public static Service makeClient(String host) {
        return ClientBuilder.safeBuild(
            ClientBuilder.get()
            .codec(new ThriftNettyCodec())
            .hosts(host)
            .hostConnectionLimit(1)
            );

    }
}
