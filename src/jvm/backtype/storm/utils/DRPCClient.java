package backtype.storm.utils;

import java.util.Map;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCClient.class);
    private DistributedRPC.Client _client;
    private String _host;
    private int _port;

    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, host, port, timeout);
        _host = host;
        _port = port;
        _client = new DistributedRPC.Client(_protocol);
    }

    public String execute(String func, String args) throws TException, DRPCExecutionException {
        return _client.execute(func, args);
    }

    public String getHost() {
        return _host;
    }

    public int getPort() {
        return _port;
    }   
}
