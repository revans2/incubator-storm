package backtype.storm.utils;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.generated.DistributedRPC;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.ThriftClient;
import org.apache.thrift7.TException;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
	private DistributedRPC.Client _client;
	private String _host;
	private int _port;

	public DRPCClient(String host, int port) {
		this(host, port, null);
	}

	public DRPCClient(String host, int port, Integer timeout) {
		super(host, port, "drpc_server", timeout);
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
