BASE_VERSION:=`cat ../VERSION`
BUILD_NUMBER?=0
FULL_VERSION="$(BASE_VERSION).$(BUILD_NUMBER)"
LEIN=LD_LIBRARY_PATH=/home/y/lib64 /home/y/bin/lein
MVN=LD_LIBRARY_PATH=/home/y/lib64 /home/y/bin/mvn

all: build

test:
	cd .. && $(MVN) -f m2-pom.xml package ${MVN_MAKE_OPTS}

build:
	cd .. && $(MVN) -f m2-pom.xml package -DskipTests ${MVN_MAKE_OPTS}

install:
	cd .. && $(MVN) -f m2-pom.xml install -DskipTests ${MVN_MAKE_OPTS}

#TODO need a good way to see if these values would have changed and update only then
BASE_VERSION: ../project.clj
	echo $(BASE_VERSION) > BASE_VERSION

RELEASE:
	echo $(FULL_VERSION) > RELEASE

testpkg: RELEASE BASE_VERSION build storm.zip
	yinst_create --buildtype test ystorm.yicf
	
package: RELEASE BASE_VERSION build storm.zip
	yinst_create --buildtype release ystorm.yicf

storm.zip: _release
	rm -f storm.zip
	cd _release && zip -r ../storm.zip *

_release: RELEASE build
	rm -rf _release
	mkdir -p _release/storm/lib
	cp ../storm-core-mvn/target/storm-core*.jar _release/storm/storm.jar
	cp ../storm-core-mvn/target/dependency/*.jar _release/storm/lib
	cp ../storm-netty-mvn/target/storm-netty*.jar _release/storm/storm-netty.jar
	cp ../storm-netty-mvn/target/dependency/*.jar _release/storm/lib
	cp ../CHANGELOG.md _release/storm/
	cp RELEASE _release/storm/
	cp -r ../logback _release/storm/
	mkdir -p _release/storm/logs
	mkdir _release/storm/conf
	cp ../conf/storm.yaml.example _release/storm/conf/storm.yaml
	cp ../conf/jaas_digest.conf _release/storm/conf/
	cp ../conf/jaas_kerberos.conf _release/storm/conf/
	cp -R ../storm-core/src/ui/public _release/storm/
	cp -R ../bin _release/storm/
	cp ../README.markdown _release/storm/
	cp ../LICENSE.html _release/storm/
	
	

#Ugly hack to remove svnlite from the state file because statcc cannot deal
# with y_subversion replacing svnlite
rhel4_build.state: rhel4_build.yidf  rhel4_build.yml
	rm -f rhel4_build.state rhel4_build.state.tmp rhel4_build.ruleset
	statecc --branch test --no-validate ./rhel4_build.yml
	grep -v svnlite rhel4_build.state > rhel4_build.state.tmp
	mv -f rhel4_build.state.tmp rhel4_build.state
	statecc-validate-state rhel4_build.state

#Ugly hack to remove svnlite from the state file because statcc cannot deal
# with y_subversion replacing svnlite
rhel6_build.state: rhel6_build.yidf  rhel6_build.yml
	rm -f rhel6_build.state rhel6_build.state.tmp rhel6_build.ruleset
	statecc --branch test ./rhel6_build.yml

clean:
	rm -rf *_build.ruleset ystorm*.tgz BASE_VERSION RELEASE _release storm.zip
	cd .. && $(MVN) -f m2-pom.xml clean ${MVN_MAKE_OPTS}


#BuildyBlocks targets
BB_RULES=/home/y/share/yahoo_cfg/buildyblocks/Make.rules
include $(BB_RULES)

#COMMIT
COMMIT_BIM=./commit.bim
commit: BB_Provision build test
commit: BB_BIM_FILE=$(COMMIT_BIM)

commit_clean: clean

#COMPONENT
COMPONENT_BIM=./component.bim
component: BB_Provision build test package
component: BB_BIM_FILE=$(COMPONENT_BIM)

git-tag:
	git tag -f -a $(FULL_VERSION) -m "yahoo version $(FULL_VERSION)"
	git push --tags

component-dist-push:: BB_BIM_FILE=./component.bim
#component-dist-push:: component BB_Dist_Push
component-dist-push:: component git-tag
	/home/y/bin/dist_install ystorm-*.tgz -batch -branch quarantine -group hadoopqa -identity /home/y/var/yhudson/keys/hadoopqa/id_rsa -tag ystorm_master_security

component_clean: clean
