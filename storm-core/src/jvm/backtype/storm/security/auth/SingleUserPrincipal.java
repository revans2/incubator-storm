package backtype.storm.security.auth;

import java.security.Principal;

/**
 * A Principal that represents a user.
 */
public class SingleUserPrincipal implements Principal {

    private final String _userName;

    public SingleUserPrincipal(String userName) {
        _userName = userName;
    }

    @Override
    public boolean equals(Object another) {
        if (another instanceof SingleUserPrincipal) {
            return _userName.equals(((SingleUserPrincipal)another)._userName);
        }
        return false;
    }

    @Override
    public String getName() {
        return _userName;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    @Override
    public int hashCode() {
        return _userName.hashCode();
    }
}
