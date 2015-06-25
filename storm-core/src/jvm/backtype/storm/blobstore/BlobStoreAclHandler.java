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

  public static final int READ = 0x01;
  public static final int WRITE = 0x02;
  public static final int ADMIN = 0x04;
  public static final List<AccessControl> WORLD_EVERYTHING =
      Arrays.asList(new AccessControl(AccessControlType.OTHER, READ | WRITE | ADMIN));
  public static final List<AccessControl> DEFAULT = new ArrayList<AccessControl>();
  private Set<String> _supervisors;
  private Set<String> _admins;

  public BlobStoreAclHandler(Map conf) {
    _ptol = AuthUtils.GetPrincipalToLocalPlugin(conf);
    _supervisors = new HashSet<String>();
    _admins = new HashSet<String>();
    if (conf.containsKey(Config.NIMBUS_SUPERVISOR_USERS)) {
      _supervisors.addAll((List<String>)conf.get(Config.NIMBUS_SUPERVISOR_USERS));
    }
    if (conf.containsKey(Config.NIMBUS_ADMINS)) {
      _admins.addAll((List<String>)conf.get(Config.NIMBUS_ADMINS));
    }
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

  private boolean isSupervisorOrAdminOrNimbus(Set<String> user, int mask)
  {
    boolean isSupervisor = false;
    boolean isAdmin = false;
    boolean isNimbus = false;
    for(String u : user) {
      if (_supervisors.contains(u)) {
        isSupervisor = true;
        break;
      }
      if (_admins.contains(u)) {
        isAdmin = true;
        break;
      }
      if (u.equals("nimbus")) {
        isNimbus = true;
        break;
      }
    }
    if (mask > 0 && !isAdmin) {
      isSupervisor = (isSupervisor && (mask == 1));
    }
    return isSupervisor || isAdmin || isNimbus;
  }

  /**
   * The user should be able to see the metadata if and only if they have any of READ, WRITE, or ADMIN
   */
  public void validateUserCanReadMeta(List<AccessControl> acl, Subject who, String key)
      throws AuthorizationException {
    Set<String> user = constructUserFromPrincipals(who);
    if (isSupervisorOrAdminOrNimbus(user, -1)) {
      return;
    }
    for (AccessControl ac : acl) {
      int allowed = getAllowed(ac, user);
      LOG.debug(" user: {} allowed: {} key: {}", user, allowed, key);
      if ((allowed & (READ | WRITE | ADMIN)) > 0) {
        return;
      }
    }
    throw new AuthorizationException(
            user + " does not have access to " + key);
  }

  public void validateACL(List<AccessControl> acl, int mask, Subject who, String key)
      throws AuthorizationException {
    Set<String> user = constructUserFromPrincipals(who);
    LOG.debug("user {}", user);
    if(isSupervisorOrAdminOrNimbus(user, mask)) {
      return;
    }
    for (AccessControl ac : acl) {
      int allowed = getAllowed(ac, user);
      mask = ~allowed & mask;
      LOG.debug(" user: {} allowed: {} disallowed: {} key: {}", user, allowed, mask, key);
    }
    if (mask == 0) {
      return;
    }
    throw new AuthorizationException(
            user + " does not have " + namedPerms(mask) + " access to " + key);
  }

  public void normalizeSettableBlobMeta(String key, SettableBlobMeta meta, Subject who, int opMask) {
    meta.set_acl(normalizeSettableACLs(key, meta.get_acl(), who, opMask));
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

  private List<AccessControl> removeBadACLs(List<AccessControl> accessControls) {
    List<AccessControl> resultAcl = new ArrayList<AccessControl>();
    for (AccessControl control : accessControls) {
      if(control.get_type().equals(AccessControlType.OTHER) && (control.get_access() == 0 )) {
        LOG.debug("Removing invalid blobstore world ACL " +
            BlobStoreAclHandler.accessControlToString(control));
        continue;
      }
      resultAcl.add(control);
    }
    return resultAcl;
  }

  private final List<AccessControl> normalizeSettableACLs(String key, List<AccessControl> acls, Subject who,
                                                    int opMask) {
    List<AccessControl> cleanAcls = removeBadACLs(acls);
    Set<String> userNames = getUserNamesFromSubject(who);
    for (String user : userNames) {
      fixACLsForUser(cleanAcls, user, opMask);
    }
    if ((who == null || userNames.isEmpty()) && !worldEverything(acls)) {
        cleanAcls.addAll(BlobStoreAclHandler.WORLD_EVERYTHING);
      LOG.debug("Access Control for key {} is normalized to world everything {}", key, cleanAcls);
      if (!acls.isEmpty())
        LOG.warn("Access control for blob with key {} is normalized to WORLD_EVERYTHING", key);
    }
    return cleanAcls;
  }

  private boolean worldEverything(List<AccessControl> acls) {
    boolean isWorldEverything = false;
    for (AccessControl acl : acls) {
      if (acl.get_type() == AccessControlType.OTHER && acl.get_access() == 7) {
        isWorldEverything = true;
        break;
      }
    }
    return isWorldEverything;
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
