package backtype.storm.security.auth;

import java.util.Map;

import javax.security.auth.Subject;

/**
 * Provides a way to automatically push credentials to a topology and to
 * retreave them in the worker.
 */
public interface IAutoCredentials {

    public void prepare(Map conf);

    /**
     * Called to populate the credentials on the client side.
     * @param credentials the credentials to be populated.
     */
    public void populateCredentials(Map<String, String> credentials);

    /**
     * Called to initially populate the subject on the worker side with credentials passed in.
     * @param subject the subject to optionally put credentials in.
     * @param credentials the credentials to be used.
     */ 
    public void populateSubject(Subject subject, Map<String, String> credentials);


    /**
     * Called to update the subject on the worker side when new credentials are recieved.
     * This means that populateSubject has already been called on this subject.  
     * @param subject the subject to optionally put credentials in.
     * @param credentials the credentials to be used.
     */ 
    public void updateSubject(Subject subject, Map<String, String> credentials);

}
