package backtype.storm.utils;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServerCnxnFactory {
	private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServerCnxnFactory.class);
	int _port;
	NIOServerCnxnFactory _factory;
	
	public ZookeeperServerCnxnFactory(int port, int maxClientCnxns)  {
		//port range
		int max;
		if (port <= 0) {
			_port = 2000;
			max = 65535;
		} else {
			_port = port;
			max = port;
		}

		try {
			_factory = new NIOServerCnxnFactory();
		} catch (IOException e) {
			_port = 0;
			_factory = null;
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
		
		//look for available port 
		for (; _port <= max; _port++) {
			try {
				_factory.configure(new InetSocketAddress(_port), maxClientCnxns);
				LOG.debug("Zookeeper server successfully binded at port "+_port);
				break;
			} catch (BindException e1) {
			} catch (IOException e2) {
				_port = 0;
				_factory = null;
				e2.printStackTrace();
				throw new RuntimeException(e2.getMessage());
			} 
		} 		

		if (_port > max) {
			_port = 0;
			_factory = null;
			LOG.error("Failed to find a port for Zookeeper");
			throw new RuntimeException("No port is available to launch an inprocess zookeeper.");
		}
	}
	
	public int port() {
		return _port;
	}
		
	public NIOServerCnxnFactory factory() {
		return _factory;
	}
}
