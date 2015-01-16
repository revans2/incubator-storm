/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.blobstore;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;

import backtype.storm.Config;
import backtype.storm.generated.AccessControl;
import backtype.storm.generated.AccessControlType;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.SettableBlobMeta;
import backtype.storm.security.auth.AuthUtils;
import backtype.storm.security.auth.IPrincipalToLocal;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides common handling of acls for Blobstores.
 * Also contains some static utility functions related to Blobstores.
 */
public class BlobStoreAclHandler {
  public static final Logger LOG = LoggerFactory.getLogger(BlobStoreAclHandler.class);
  private final IPrincipalToLocal _ptol;
  private final AccessControl _superACL;

  public static final int READ = 0x01;
  public static final int WRITE = 0x02;
  public static final int ADMIN = 0x04;
  public static final List<AccessControl> WORLD_EVERYTHING =
      Arrays.asList(new AccessControl(AccessControlType.OTHER, READ | WRITE | ADMIN));

  public BlobStoreAclHandler(Map conf) {
    _superACL = getSuperAcl(conf);
    _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
  }

  private static AccessControlType parseACLType(String type) {
    if ("other".equalsIgnoreCase(type) || "o".equalsIgnoreCase(type)) {
      return AccessControlType.OTHER;
    } else if ("user".equalsIgnoreCase(type) || "u".equalsIgnoreCase(type)) {
      return AccessControlType.USER;
    }
    throw new IllegalArgumentException(type+" is not a valid access control type");
  }

  private static int parseAccess(String access) {
    int ret = 0;
    for (char c: access.toCharArray()) {
      if ('r' == c) {
        ret = ret | READ;
      } else if ('w' == c) {
        ret = ret | WRITE;
      } else if ('a' == c) {
        ret = ret | ADMIN;
      } else if ('-' == c) {
        //ignored
      } else {
        throw new IllegalArgumentException("");
      }
    }
    return ret;
  }

  public static AccessControl parseAccessControl(String str) {
    String[] parts = str.split(":");
    String type = "other";
    String name = "";
    String access = "-";
    if (parts.length > 3) {
      throw new IllegalArgumentException("Don't know how to parse "+str+" into an ACL value");
    } else if (parts.length == 1) {
      type = "other";
      name = "";
      access = parts[0];
    } else if (parts.length == 2) {
      type = "user";
      name = parts[0];
      access = parts[1];
    } else if (parts.length == 3) {
      type = parts[0];
      name = parts[1];
      access = parts[2];
    }
    AccessControl ret = new AccessControl();
    ret.set_type(parseACLType(type));
    ret.set_name(name);
    ret.set_access(parseAccess(access));
    return ret;
  }

  private static String accessToString(int access) {
    StringBuffer ret = new StringBuffer();
    ret.append(((access & READ) > 0) ? "r" : "-");
    ret.append(((access & WRITE) > 0) ? "w" : "-");
    ret.append(((access & ADMIN) > 0) ? "a" : "-");
    return ret.toString();
  }

  public static String accessControlToString(AccessControl ac) {
    StringBuffer ret = new StringBuffer();
    switch(ac.get_type()) {
      case OTHER:
        ret.append("o");
        break;
      case USER:
        ret.append("u");
        break;
      default:
        throw new IllegalArgumentException("Ahh don't know what a type of "+ac.get_type()+" means ");
    }
    ret.append(":");
    if (ac.is_set_name()) {
      ret.append(ac.get_name());
    }
    ret.append(":");
    ret.append(accessToString(ac.get_access()));
    return ret.toString();
  }

  public static void validateSettableACLs(String key, List<AccessControl> acls) throws AuthorizationException {
    Set<String> aclUsers = new HashSet<>();
    List<String> duplicateUsers = new ArrayList<>();
    for (AccessControl acl : acls) {
      String aclUser = acl.get_name();
      if (aclUser != null && !aclUser.isEmpty() && !aclUsers.add(aclUser)) {
        LOG.error("'{}' user can't appear more than once in the ACLs", aclUser);
        duplicateUsers.add(aclUser);
      }
    }
    if (duplicateUsers.size() > 0) {
      String errorMessage  = "user " + Arrays.toString(duplicateUsers.toArray())
          + " can't appear more than once in the ACLs for key [" + key +"].";
      throw new AuthorizationException(errorMessage);
    }
  }

  public static String getBlobStoreSuperUser(Map conf) {
    return (String) conf.get(Config.BLOBSTORE_SUPERUSER);
  }

  protected AccessControl getSuperAcl(Map conf) {
    String user = getBlobStoreSuperUser(conf);
    AccessControl acl = new AccessControl();
    if (user == null || user.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid configuration: " + Config.BLOBSTORE_SUPERUSER + " not set.");
    }
    acl.set_name(user);
    acl.set_type(AccessControlType.USER);
    acl.set_access(READ | WRITE | ADMIN);
    return acl;
  }


  private Set<String> constructUserFromPrincipals(Subject who) {
    Set<String> user = new HashSet<String>();
    if (who == null) {
      LOG.debug("in validate acl who is null");
    } else {
      LOG.debug("in validate acl: " + who);
    }
    if (who != null) {
      for (Principal p : who.getPrincipals()) {
        user.add(_ptol.toLocal(p));
      }
    }
    return user;
  }
 
  /**
   * The user should be able to see the metadata if they have any of READ, WRITE, or ADMIN
   */
  public void validateUserCanReadMeta(List<AccessControl> acl, Subject who, String key)
      throws AuthorizationException {
    Set<String> user = constructUserFromPrincipals(who);
    boolean hasAccess = false;
    for (AccessControl ac : acl) {
      int allowed = getAllowed(ac, user);
      LOG.debug(" user: " + user + " allowed: " + allowed + " key: " + key);
      if ((allowed & (READ | WRITE | ADMIN)) > 0) {
        hasAccess = true;
        break;
      }
    }
    if (!hasAccess) {
      throw new AuthorizationException(
          user + " does not have access to " + key);
    }
  }

  public void validateACL(List<AccessControl> acl, int mask, Subject who, String key)
      throws AuthorizationException {
    Set<String> user = constructUserFromPrincipals(who);

    for (AccessControl ac : acl) {
      int allowed = getAllowed(ac, user);
      mask = ~allowed & mask;
      LOG.debug(" user: " + user + " allowed: " + allowed + " mask: + " + mask + " key: " + key);
    }
    if (mask != 0) {
      throw new AuthorizationException(
          user + " does not have " + namedPerms(mask) + " access to " + key);
    }

  }

  public void normalizeSettableBlobMeta(SettableBlobMeta meta, Subject who, int opMask) {
    meta.set_acl(normalizeSettableACLs(meta.get_acl(), who, opMask));
  }

  private String namedPerms(int mask) {
    StringBuffer b = new StringBuffer();
    b.append("[");
    if ((mask & READ) > 0) {
      b.append("READ ");
    }
    if ((mask & WRITE) > 0) {
      b.append("WRITE ");
    }
    if ((mask & ADMIN) > 0) {
      b.append("ADMIN ");
    }
    b.append("]");
    return b.toString();
  }

  private int getAllowed(AccessControl ac, Set<String> users) {
    switch (ac.get_type()) {
      case OTHER:
        return ac.get_access();
      case USER:
        if (users.contains(ac.get_name())) {
          return ac.get_access();
        }
        return 0;
      default:
        return 0;
    }
  }


  private List<AccessControl> removeBadACLs(List<AccessControl> accessControls,
                                              String superUser) {
    List<AccessControl> resultAcl = new ArrayList<AccessControl>();
    for (AccessControl control : accessControls) {
      int access = control.get_access();
      if (control.get_type().equals(AccessControlType.USER) &&
          control.get_name().equals(superUser) &&
          (access & ( READ | WRITE | ADMIN)) != ( READ | WRITE | ADMIN )) {
        LOG.debug("Removing invalid blobstore superuser ACL " +
            BlobStoreAclHandler.accessControlToString(control));
        continue;
      }
      if(control.get_type().equals(AccessControlType.OTHER) && (control.get_access() == 0 )) {
        LOG.debug("Removing invalid blobstore world ACL " +
            BlobStoreAclHandler.accessControlToString(control));
        continue;
      }
      resultAcl.add(control);
    }
    return resultAcl;
  }

  private boolean hasSuperACL(List<AccessControl> accessControls) {
    for (AccessControl control : accessControls) {
      if (_superACL.equals(control)) {
        return true;
      }
    }
    return false;
  }

  private final List<AccessControl> normalizeSettableACLs(List<AccessControl> acls, Subject who,
                                                    int opMask) {
    List<AccessControl> cleanAcls = removeBadACLs(acls, _superACL.get_name());
    if (!hasSuperACL(cleanAcls)) {
      LOG.info("Adding Super ACL " + BlobStoreAclHandler.accessControlToString(_superACL));
      cleanAcls.add(_superACL);
    }
    Set<String> userNames = getUserNamesFromSubject(who);
    for (String user : userNames) {
      fixACLsForUser(cleanAcls, user, opMask);
    }
    return cleanAcls;
  }

  private void fixACLsForUser(List<AccessControl> acls, String user, int mask) {
    boolean foundUserACL = false;
    for (AccessControl control : acls) {
      if (control.get_type() == AccessControlType.USER && control.get_name().equals(user)) {
        int currentAccess = control.get_access();
        if ((currentAccess & mask) != mask) {
          control.set_access(currentAccess | mask);
        }
        foundUserACL = true;
        break;
      }
    }
    if (!foundUserACL) {
      AccessControl userACL = new AccessControl();
      userACL.set_type(AccessControlType.USER);
      userACL.set_name(user);
      userACL.set_access(mask);
      acls.add(userACL);
    }
  }

  private Set<String> getUserNamesFromSubject(Subject who) {
    Set<String> user = new HashSet<String>();
    if (who != null) {
      for(Principal p: who.getPrincipals()) {
        user.add(_ptol.toLocal(p));
      }
    }
    return user;
  }
}
