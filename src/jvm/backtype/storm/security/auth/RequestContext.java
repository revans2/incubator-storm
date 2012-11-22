package backtype.storm.security.auth;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

public class RequestContext {
	
	static public Principal clientPrincipal() {
		AccessControlContext acl_ctxt = AccessController.getContext();
		Subject subject = Subject.getSubject(acl_ctxt);
		if (subject == null) return null;
		Set<Principal> princs = subject.getPrincipals();
		if (princs.size()==0) return null;
		return (Principal) (princs.toArray()[0]);
	}
}
