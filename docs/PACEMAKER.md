Storm Pacemaker
===============
Storm Pacemaker is a daemon designed to address the scaling issues associated with storing heartbeats in ZooKeeper.

Getting it Working
------------------
There are a few new configs associated with pacemaker. Most of them can be left as default. 
The following are the ones that must be set to get pacemaker working:

 * `pacemaker.host` - The host your Pacemaker instance is running on

 * `storm.cluster.state.store` - Must be set to `"pacemaker"`
 * `pacemaker.auth.method` - Can be `"DIGEST"` or `"KERBEROS"` on the server. Client must match server or be `"NONE"` (further explanation below)
 * `java.security.auth.login.config` - Must be a JAAS conf with the proper sections.

### Security
Pacemaker runs with security. Anyone can write to Pacemaker, but only authenticated users can read. 
At the moment, there is only rough access control for Kerberos, explained below.

The two methods available to authenticate are `DIGEST` and `KERBEROS`

Nimbus should be configured to authenticate to Pacemaker, but the workers should not be.

That is,
**Nimbus's `pacemaker.auth.method` must match Pacemaker's `pacemaker.auth.method`**

###### DIGEST
Digest uses a simple username/password pair. Inside the JAAS conf (pointed to by `java.security.auth.login.config`) on both the Nimbus node and the Pacemaker node, there must
be a section like the following:

```
PacemakerDigest {
      org.apache.zookeeper.server.auth.DigestLoginModule required
      username="nimbus"
      password="secret_nimbus_password";
};
```

###### KERBEROS
Kerberos is more complicated (surprise!)

For kerberos to work, two sections must appear in the JAAS conf:
```
PacemakerServer {
       com.sun.security.auth.module.Krb5LoginModule required
       useKeyTab=true
       keyTab="/etc/pacemaker.keytab"
       storeKey=true
       useTicketCache=false
       principal="pacemaker/my.pacemaker.host@STORM.ORGANIZATION.COM";
};
PacemakerClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="/etc/nimbus_server.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="pacemaker"
   principal="nimbus_server/my.nimbus.host@STORM.ORGANIZATION.COM";
};
```
A couple things:
 * The keytabs must be in place and readable by the user running the storm daemons.
 * The `serviceName` in `PacemakerClient` must match the bit of the principal before the slash in `PacemakerServer` (in this case, `pacemaker`)

There is another config that needs to be set, `pacemaker.kerberos.users`, which is a list of principals allows to access Pacemaker.
In this case, it would just be:
```
pacemaker.kerberos.users:
    - "nimbus_server/my.nimbus.host@STORM.ORGANIZATION.COM"
```
This is the only access control mechanism included in Pacemaker currently.

Once all this is configured, just launch pacemaker somewhere with the `storm pacemaker` command.

That's it! Check Pacemaker logs to make sure workers it's configured properly.

Storm Client
------------

The storm client includes a new command, `heartbeats`, which can be used like follows:

```
-bash-4.1$ storm heartbeats list /
...
4262 [main] INFO  b.s.c.heartbeats - list /:
workerbeats

-bash-4.1$ storm heartbeats list /workerbeats
...
4252 [main] INFO  b.s.c.heartbeats - list /workerbeats:
test-1-1434561979

-bash-4.1$ storm heartbeats list /workerbeats/test-1-1434561979
...
4240 [main] INFO  b.s.c.heartbeats - list /workerbeats/test-1-1434561979:
77db060e-2f94-4b21-b04b-a70ab359074a-6700
77db060e-2f94-4b21-b04b-a70ab359074a-6703
77db060e-2f94-4b21-b04b-a70ab359074a-6701

-bash-4.1$ storm heartbeats list /workerbeats/test-1-1434561979/77db060e-2f94-4b21-b04b-a70ab359074a-6703
...
4359 [main] INFO  b.s.c.heartbeats - 
{:storm-id "test-1-1434561979"
 :executor-stats {(14 14) #backtype.storm.stats.CommonStats{:emitted {"10800" {"__metrics" 20
                                                                               "default" 15760}
                                                                      "86400" {"__metrics" 20
                                                                               "default" 15760}
                                                                      ":all-time" {"__metrics" 20
                                                                                   "default" 15760}
                                                                      "600" {"default" 5860}}
                                                            :transferred {"10800" {"__metrics" 0
                                                                                   "default" 15760}
                                                                          "86400" {"__metrics" 0
                                                                                   "default" 15760}
                                                                          ":all-time" {"__metrics" 0
                                                                                       "default" 15760}
                                                                          "600" {"default" 5860}}
                                                            :rate 20.0
                                                            :complete-latencies {"10800" {}
                                                                                 "86400" {}
                                                                                 ":all-time" {}
                                                                                 "600" {}}
                                                            :failed {"10800" {}
                                                                     "86400" {}
                                                                     ":all-time" {}
                                                                     "600" {}}
                                                            :acked {"10800" {}
                                                                    "86400" {}
                                                                    ":all-time" {}
                                                                    "600" {}}
                                                            :type :spout}
                  (6 6) #backtype.storm.stats.CommonStats{:emitted {"10800" {"default" 55020}
                                                                    "86400" {"default" 55020}
                                                                    ":all-time" {"default" 55020}
                                                                    "600" {"default" 20400}}
                                                          :transferred {"10800" {"default" 55020}
                                                                        "86400" {"default" 55020}
                                                                        ":all-time" {"default" 55020}
                                                                        "600" {"default" 20400}}
                                                          :rate 20.0
                                                          :execute-latencies {"10800" {["word" "default"] 0.2131684248817752}
                                                                              "86400" {["word" "default"] 0.2131684248817752}
                                                                              ":all-time" {["word" "default"] 0.2131684248817752}
                                                                              "600" {["word" "default"] 0.2080471050049068}}
                                                          :executed {"10800" {["word" "default"] 54980}
                                                                     "86400" {["word" "default"] 54980}
                                                                     ":all-time" {["word" "default"] 54980}
                                                                     "600" {["word" "default"] 20380}}
                                                          :process-latencies {"10800" {["word" "default"] 0.1142233539468898}
                                                                              "86400" {["word" "default"] 0.1142233539468898}
                                                                              ":all-time" {["word" "default"] 0.1142233539468898}
                                                                              "600" {["word" "default"] 0.1177625122669284}}
                                                          :failed {"10800" {}
                                                                   "86400" {}
                                                                   ":all-time" {}
                                                                   "600" {}}
                                                          :acked {"10800" {["word" "default"] 54980}
                                                                  "86400" {["word" "default"] 54980}
                                                                  ":all-time" {["word" "default"] 54980}
                                                                  "600" {["word" "default"] 20380}}
                                                          :type :bolt}}
 ...
 :uptime 1582
 :time-secs 1434563567}
```

### Security

The security configuration is the same for the storm client as it is for Nimbus (It uses the same code)
The storm conf must have `pacemaker.auth.method` set properly, and `java.security.auth.login.config` must be set properly, or be passed to the storm client with -c
```
storm heartbeats get /workerbeats/test-1-1434561979/77db060e-2f94-4b21-b04b-a70ab359074a-6700 -c java.security.auth.login.config=/etc/storm/jaas.conf
```

Performance
-----------

Performance testing was done on Pacemaker, and the determination was that a single Pacemaker node was sufficient to remove ZooKeeper as a scaling bottleneck. 

Brought up 277 Supervisor nodes, a Nimbus/UI node, and a Pacemaker node.
Supervisors were adjusted to 24 workers per node for a total of 6648 slots.

 * Launching ~400 topologies, filling all slots, Pacemaker served ~2000 heartbeats per second. Memory and CPU utilization were low.
 * Launching ~100 topologies (large), filling all slots, Pacemaker served about the same amount. Memory and CPU utilization were low.
 * Launching 1 huge topology, heartbeats grew in size to (compressed) 29k. Again, CPU and Memory utilization were low.

Scaling beyond this point was limited by Nimbus, which froze up when trying to schedule larger topologies. A follow-on issue will be filed to address these scaling problems.

#### Future

While a single Pacemaker node is sufficient for now, it does provide a 'single point of failure' in the system. Next steps should be to modify `pacemaker_state_factory.clj` to deal with multiple Pacemaker instances.
