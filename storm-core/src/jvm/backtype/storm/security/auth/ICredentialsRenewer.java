package backtype.storm.security.auth;

import java.util.Collection;
import java.util.Map;

/**
 * Provides a way to renew credentials on behelf of a user.
 */
public interface ICredentialsRenewer {

   /**
    * Called when initializing the service.
    * @param conf the storm cluster configuration.
    */ 
   public void prepare(Map conf);

    /**
     * Renew any credentials that need to be renewed. (Update the credentials if needed)
     * @param credentials the credentials that may have something to renew.
     */ 
    public void renew(Map<String, String> credentials);
}
