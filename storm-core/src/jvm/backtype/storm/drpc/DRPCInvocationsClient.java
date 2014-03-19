package backtype.storm.drpc;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DRPCInvocationsClient extends ThriftClient implements DistributedRPCInvocations.Iface {
    public static Logger LOG = LoggerFactory.getLogger(DRPCInvocationsClient.class);
    private final AtomicReference<DistributedRPCInvocations.Client> client =
       new AtomicReference<DistributedRPCInvocations.Client>();
    private String host;
    private int port;

    public DRPCInvocationsClient(Map conf, String host, int port) throws TTransportException {
        super(conf, host, port, null);
        this.host = host;
        this.port = port;
        client.set(new DistributedRPCInvocations.Client(_protocol));
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }       

    public void reconnectClient() throws TException {
        if (client.get() == null) {
            reconnect();
            client.set(new DistributedRPCInvocations.Client(_protocol));
        }
    }

    public boolean isConnected() {
        return client.get() != null;
    }

    public void result(String id, String result) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            c.result(id, result);
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }

    public DRPCRequest fetchRequest(String func) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            return c.fetchRequest(func);
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }    

    public void failRequest(String id) throws TException, AuthorizationException {
        DistributedRPCInvocations.Client c = client.get();
        try {
            if (c == null) {
                throw new TException("Client is not connected...");
            }
            c.failRequest(id);
        } catch(TException e) {
            client.compareAndSet(c, null);
            throw e;
        }
    }

    public DistributedRPCInvocations.Client getClient() {
        return client.get();
    }
}
