package backtype.storm;
import java.util.Map;

import java.util.Map;

/**
 * Allows a bolt or a spout to be informed when the credentials of the topology have changed.
 */
public interface ICredentialsListener {
    /**
     * Called when the credentials of a topology have changed.
     * @param credentials the new credentials.
     */
    public void credentialsHaveChanged(Map<String,String> credentials);
}
