package backtype.storm.drpc;

import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.security.auth.ThriftClient;

import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

public class DRPCInvocationsClient extends ThriftClient implements DistributedRPCInvocations.Iface {
    public static final Logger LOG = Logger.getLogger(DRPCInvocationsClient.class);
	private DistributedRPCInvocations.Client _client;
	private String _host;
	private int _port;    

	public DRPCInvocationsClient(String host, int port) {
		super(host, port, "drpc_server", null);
		_host = host;
		_port = port;
		_client = new DistributedRPCInvocations.Client(_protocol);
	}

	public String getHost() {
		return _host;
	}

	public int getPort() {
		return _port;
	}       

	public void result(String id, String result) throws TException {
		try {
			//if(_client==null) connect();
			_client.result(id, result);
		} catch(TException e) {
			LOG.error("result() exception "+e, e);
			throw e;
		}
	}

	public DRPCRequest fetchRequest(String func) throws TException {
		try {
			//if(_client==null) connect();
			return _client.fetchRequest(func);
		} catch(TException e) {
			LOG.error("fetchRequest() exception "+e, e);
			throw e;
		}
	}    

	public void failRequest(String id) throws TException {
		try {
			//if(_client==null) connect();
			_client.failRequest(id);
		} catch(TException e) {
			LOG.error("failRequest() exception "+e, e);
			throw e;
		}
	}
}
