# storm-yahoo

Yahoo-specific plugins to storm

To test, run `LD_LIBRARY_PATH=/home/y/lib64 mvn test`

# Developer instructions for YSTORM
### Creating open stack instances
Go to ```yo/openhouse``` (https://openhouse.corp.yahoo.com) and choose any of the region.
## Development Environment Setup
#### Install Java & Maven
```
yinst i yjava_maven
```

For Yahoo specific distributions head to ```yo/dist``` and read about [yinst documentation](http://devel.yahoo.com/yinst/guide/yinst.pdf)

More details on setting up storm cluster available at
* [StormQuickStart](http://twiki.corp.yahoo.com/view/Grid/StormQuickStart)
* [MultiTenantStormSetup](http://twiki.corp.yahoo.com/view/Grid/MultiTenantStormSetup)


To clone your repo and build on the openstack (```yo/instances```), add the openstack server's SSH key to the github profile ```https://help.github.com/articles/generating-ssh-keys/``` 
or login to the openstack instance through ```ssh -A <host>``` command without needing to add to the profile every time a new instance is created. 


## Development on Mac     
###  For community Storm or 0.10 YSTORM and upwards
Install the following packages for running community Storm or for 0.10 and above : [ listed commands for mac)
* Install brew through ```http://brew.sh/```

Then install the following packages

```
brew install node
brew install thrift
brew install maven 
brew install ruby
gem install json
```

Incase you are developing on your ```YAHOO Mac over wifi``` (only on WiFi). you may need to override this flag during install to overcome some issue with Yahoo - OSx IPv6 related problem

The default ```test.extra.args``` in pom.xml looks like ```<test.extra.args>-Djava.net.preferIPv4Stack=true</test.extra.arg```>
and needs to be overriden. You may use command line args like below during install or change the pom.

```
mvn clean install -Dtest.extra.args='-Dignored'
```

### Running clojure unit tests via REPL
From within ```storm-core``` directory, we can run individual clojure tests like below
* Invoke the REPL
``` bash
$ mvn clojure:repl
```

* from within REPL
```
REPL=> (use 'clojure.test)
```

To run all tests in the clojure code.
eg: running the ***multitenant-scheduler-test***
```
(require 'backtype.storm.scheduler.multitenant-scheduler-test :reload-all)
(run-tests 'backtype.storm.scheduler.multitenant-scheduler-test)
```

You can look here for [more details](http://clojure.github.io/clojure/clojure.test-api.html).
