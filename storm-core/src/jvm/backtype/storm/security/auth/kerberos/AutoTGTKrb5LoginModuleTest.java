package backtype.storm.security.auth.kerberos;

import java.security.Principal;
import javax.security.auth.kerberos.KerberosTicket;

/**
 * Custom LoginModule to enable Auto Login based on cached ticket
 */
public class AutoTGTKrb5LoginModuleTest extends AutoTGTKrb5LoginModule {

    public Principal client = null;

    public void setKerbTicket(KerberosTicket ticket) {
        this.kerbTicket = ticket;
    }
    
    @Override
    protected void getKerbTicketFromCache() {
        // Do nothing.
    }

    @Override
    protected Principal getKerbTicketClient() {
        return this.client;
    }
}
