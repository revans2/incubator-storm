package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Map;
import javax.security.auth.login.Configuration;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.protocol.TProtocol;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import backtype.storm.utils.Utils;
import backtype.storm.Config;
import backtype.storm.security.auth.TBackoffConnect;

public class ThriftClient {	
    private static final Logger LOG = LoggerFactory.getLogger(ThriftClient.class);
    private TTransport _transport;
    protected TProtocol _protocol;
    private String host;
    private int port;
    private Integer timeout;
    private ITransportPlugin transportPlugin;
    private Map storm_conf;

    public ThriftClient(Map storm_conf, String host, int port) {
        this(storm_conf, host, port, null);
    }

    public ThriftClient(Map storm_conf, String host, int port, Integer timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.storm_conf = storm_conf;
        //locate login configuration 
        Configuration login_conf = AuthUtils.GetConfiguration(storm_conf);

        //construct a transport plugin
        this.transportPlugin = AuthUtils.GetTransportPlugin(storm_conf, login_conf, null);

        //create a socket with server
        if (host==null) {
            throw new IllegalArgumentException("host is not set");
        }
        if (port<=0) {
            throw new IllegalArgumentException("invalid port: "+port);
        }
        reconnect();
    }

    public synchronized TTransport transport() {
        return _transport;
    }
    
    public synchronized void reconnect() {
        close();    
        try {
            TSocket socket = new TSocket(host, port);
            if(timeout!=null) {
                socket.setTimeout(timeout);
            }
            final TTransport underlyingTransport = socket;

            //establish client-server transport via plugin
            //do retries if the connect fails
            TBackoffConnect connectionRetry 
                = new TBackoffConnect(
                                      Utils.getInt(storm_conf.get(Config.STORM_NIMBUS_RETRY_TIMES)),
                                      Utils.getInt(storm_conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL)),
                                      Utils.getInt(storm_conf.get(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING)));
            _transport = connectionRetry.doConnectWithRetry(transportPlugin, underlyingTransport, host);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        _protocol = null;
        if (_transport != null) {
            _protocol = new  TBinaryProtocol(_transport);
        }
    }

    public synchronized void close() {
        if (_transport != null) {
            _transport.close();
            _transport = null;
            _protocol = null;
        }
    }
}
