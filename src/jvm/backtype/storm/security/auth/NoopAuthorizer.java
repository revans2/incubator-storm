package backtype.storm.security.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A no-op authorization implementation that illustrate info available for authorization decisions.
 * 
 * @author afeng
 *
 */
public class NoopAuthorizer implements IAuthorization {
	private static final Logger LOG = LoggerFactory.getLogger(NoopAuthorizer.class);

	/**
	 * permit() method is invoked for each incoming Thrift request
	 * @param contrext request context includes info about 
	 *      		   (1) remote address/subject, 
	 *                 (2) operation
	 *                 (3) configuration of targeted topology 
	 * @return true if the request is authorized, false if reject
	 */
	public boolean permit(ReqContext context) {
		LOG.info("Authorized "
				+ " from: " + context.remoteAddress().toString()
				+ " principal:"+context.principal()
				+" op:"+context.operation()
				+ " topoology conf="+context.topologyConf());
		return true;
	}
}
