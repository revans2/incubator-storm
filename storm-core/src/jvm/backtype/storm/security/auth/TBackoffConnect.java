package backtype.storm.security.auth;

import java.io.IOException;
import java.util.Random;
import org.apache.thrift7.transport.TTransport;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TBackoffConnect {
    private static final Logger LOG = LoggerFactory.getLogger(TBackoffConnect.class);
    private int _completedRetries = 0;
    private int _retryTimes;
    private int _retryInterval;
    private int _retryIntervalCeiling;
    private int _retryRandomRange;
    private Random random;
    public TBackoffConnect(int retryTimes, int retryInterval, int retryIntervalCeiling, int retryRandomRange) {
	_retryTimes = retryTimes;
	_retryInterval = retryInterval;
	_retryIntervalCeiling = retryIntervalCeiling;
	_retryRandomRange = retryRandomRange;
	random = new Random();
    }

    public TTransport doConnectWithRetry(ITransportPlugin transportPlugin, TTransport underlyingTransport, String host) throws IOException {
	boolean connected = false;
	TTransport transportResult = null;
	while(!connected) {
	    try {
		transportResult = transportPlugin.connect(underlyingTransport, host);
		connected = true;
	    } catch (TTransportException ex) {
		retryNext(ex);
	    }
	}
	return transportResult;
    }

    private void retryNext(TTransportException ex) {
	if(!canRetry()) {
	    throw new RuntimeException(ex);
	}
	try {
	    int sleeptime = getNextSleepTimeMs();

	    LOG.debug("Failed to connect. Retrying... (" + Integer.toString( _completedRetries) + ") in " + Integer.toString(sleeptime) + "ms");

	    Thread.sleep(sleeptime);
	} catch (InterruptedException e) {}

	_completedRetries++;
    }

    private boolean canRetry() {
	return (_completedRetries < _retryTimes);
    }

    private int getNextSleepTimeMs()
    {
	int nextsleep = Math.max(_retryInterval
				 + (random.nextInt(_retryRandomRange * 2) - _retryRandomRange),
				 0);

	_retryInterval = Math.min( (_retryInterval * _retryInterval / 1000), 
				   _retryIntervalCeiling);
	
	return nextsleep;
    }
}