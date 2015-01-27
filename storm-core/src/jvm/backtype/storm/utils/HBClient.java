package backtype.storm.utils;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.HBExecutionException;
import backtype.storm.generated.HBServer;
import backtype.storm.generated.Nimbus;
import backtype.storm.security.auth.ThriftClient;
import backtype.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Map;

public class HBClient extends ThriftClient {
    private HBServer.Client _client;
    private static final Logger LOG = LoggerFactory.getLogger(HBClient.class);

    public static HBClient getConfiguredClient(Map conf) {
        try {
            String hbHost = (String) conf.get(Config.HBSERVER_HOST);
            return new HBClient(conf, hbHost);
        } catch (TTransportException ex) {
            throw new RuntimeException(ex);
        }
    }

    public HBClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public HBClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.HBSERVER, host, port, timeout);
        _client = new HBServer.Client(_protocol);
    }

    public HBClient(Map conf, String host) throws TTransportException {
        super(conf, ThriftConnectionType.HBSERVER, host, null, null);
        _client = new HBServer.Client(_protocol);
    }

    public HBServer.Client getClient() {
        return _client;
    }

}
