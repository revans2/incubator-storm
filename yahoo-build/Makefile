BASE_VERSION:=`cat BASE_VERSION`
MVN_BASE_VERSION:=`cat yahoo-build/BASE_VERSION`
FULL_VERSION=`cat RELEASE`
BASE_VERSION_UNDERSCORE=`cat BASE_VERSION_UNDERSCORE`
MVN_FULL_VERSION=`cat yahoo-build/RELEASE`
MVN=LD_LIBRARY_PATH=/home/y/lib64 /home/y/bin/mvn
EXEC_CONF_DIR?=/home/y/conf/storm/

all: build

test:
	cd .. && $(MVN) -Pnative -Pcoverage -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} package ${MVN_MAKE_OPTS}

check: test
	@echo "Errors or Failures:"
	@egrep -il '<fail|<error' ../*/target/test-reports/*.xml | xargs -I '{}' bash -c "echo -n '{}':' '; egrep -ic '<fail|<error' '{}'"
	@! egrep -iq '<fail|<error' ../*/target/test-reports/*.xml 


build:
	cd .. && $(MVN) -Pnative -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} package -DskipTests ${MVN_MAKE_OPTS}

install:
	cd .. && $(MVN) -Pnative -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} install -DskipTests ${MVN_MAKE_OPTS}

#TODO need a good way to see if these values would have changed and update only then
BASE_VERSION: ../pom.xml
	cd .. && $(MVN) help:evaluate -Dexpression=project.version | grep -v "^\[" | tail -1 > yahoo-build/BASE_VERSION

BASE_VERSION_UNDERSCORE: BASE_VERSION
	echo "$(BASE_VERSION)" | sed -e 's/-/_/g' > BASE_VERSION_UNDERSCORE

RELEASE: BASE_VERSION_UNDERSCORE
	/home/y/bin/auto_increment_version.pl ystorm $(BASE_VERSION_UNDERSCORE) > RELEASE

testpkg: RELEASE BASE_VERSION build
	cd .. && ${MVN} versions:set -DnewVersion=$(MVN_FULL_VERSION)
	cd .. && ${MVN} versions:update-child-modules
	cd .. && find . -name pom.xml | xargs git update-index --assume-unchanged
	yinst_create --buildtype test ystorm.yicf
	cd .. && ${MVN} versions:revert
	cd .. && ${MVN} versions:update-child-modules

	
# This target is called from screwdriver build and leaves the poms changed for its
# publish phase
package-sd: RELEASE build
	cd .. && ${MVN} versions:set -DnewVersion=$(MVN_FULL_VERSION)
	cd .. && ${MVN} versions:update-child-modules
	cd .. && find . -name pom.xml | xargs git update-index --assume-unchanged
	yinst_create --buildtype release ystorm.yicf

package: RELEASE build
	cd .. && ${MVN} versions:set -DnewVersion=$(MVN_FULL_VERSION)
	cd .. && ${MVN} versions:update-child-modules
	cd .. && find . -name pom.xml | xargs git update-index --assume-unchanged
	yinst_create --buildtype release ystorm.yicf
	cd .. && ${MVN} versions:revert
	cd .. && ${MVN} versions:update-child-modules


rhel6_build.state: rhel6_build.yidf rhel6_build.yml
	rm -f rhel6_build.state rhel6_build.state.tmp rhel6_build.ruleset
	statecc --branch test ./rhel6_build.yml

clean:
	cd .. && $(MVN) clean ${MVN_MAKE_OPTS}
	cd .. && ${MVN} versions:revert
	cd .. && ${MVN} versions:update-child-modules
	rm -rf *_build.ruleset ystorm*.tgz BASE_VERSION RELEASE BASE_VERSION_UNDERSCORE


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

git-tag: RELEASE
	git tag -f -a $(FULL_VERSION) -m "yahoo version $(FULL_VERSION)"
	git push origin $(FULL_VERSION)

component-dist-push:: BB_BIM_FILE=./component.bim
#component-dist-push:: component BB_Dist_Push
component-dist-push:: component git-tag
	/home/y/bin/dist_install ystorm-*.tgz -batch -branch quarantine -group hadoopqa -identity /home/y/var/yhudson/keys/hadoopqa/id_rsa -tag ystorm_master_security

component_clean: clean
