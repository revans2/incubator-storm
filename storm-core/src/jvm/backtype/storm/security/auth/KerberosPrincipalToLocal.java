package backtype.storm.security.auth;

import java.util.Map;
import java.security.Principal;

/**
 * Map a kerberos principal to a local user
 */
public class KerberosPrincipalToLocal implements IPrincipalToLocal {

    /**
     * Invoked once immediately after construction
     * @param conf Storm configuration 
     */
    public void prepare(Map storm_conf) {}
    
    /**
     * Convert a Principal to a local user name.
     * @param principal the principal to convert
     * @return The local user name.
     */
    public String toLocal(Principal principal) {
      //This technically does not conform with rfc1964, but should work so
      // long as you don't have any really odd names in your KDC.
      return principal == null ? null : principal.getName().split("[/@]")[0];
    }
}
