package backtype.storm;

import java.util.Map;

/**
 * Allows a bolt or a spout to be informed when the credentials of the topology have changed.
 */
public interface ICredentialsListener {
    /**
     * Called when the credentials of a topology have changed.
     * @param credentials the new credentials, could be null.
     */
    public void setCredentials(Map<String,String> credentials);
}
