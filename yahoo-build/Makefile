BASE_VERSION:=`cat BASE_VERSION`
MVN_BASE_VERSION:=`cat yahoo-build/BASE_VERSION`
FULL_VERSION=`cat RELEASE`
BASE_VERSION_UNDERSCORE=`cat BASE_VERSION_UNDERSCORE`
MVN_FULL_VERSION=`cat yahoo-build/RELEASE`
MVN=LD_LIBRARY_PATH=/home/y/lib64 /home/y/bin/mvn
EXEC_CONF_DIR?=/home/y/conf/storm/

all: build

test: RELEASE
	cd .. && $(MVN) -Pnative -Pstorm-core -Pcoverage -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} install ${MVN_MAKE_OPTS} -Dystorm.build.number=$(shell sed 's;.*\.;;' RELEASE)
	cd .. && $(MVN) -Pyahoo -Pstorm-more -Pcoverage -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} package ${MVN_MAKE_OPTS} -Dystorm.build.number=$(shell sed 's;.*\.;;' RELEASE)

check: test
	@echo "Errors or Failures:"
	@egrep -il '<fail|<error' ../*/target/test-reports/*.xml | xargs -I '{}' bash -c "echo -n '{}':' '; egrep -ic '<fail|<error' '{}'"
	@! egrep -iq '<fail|<error' ../*/target/test-reports/*.xml 


build: RELEASE
	cd .. && $(MVN) -Pnative -Pstorm-core -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} install -DskipTests ${MVN_MAKE_OPTS} -Dystorm.build.number=$(shell sed 's;.*\.;;' RELEASE)
	cd .. && $(MVN) -Pyahoo -Pnative -Pstorm-more -Dworker-launcher.conf.dir=${EXEC_CONF_DIR} package -DskipTests ${MVN_MAKE_OPTS} -Dystorm.build.number=$(shell sed 's;.*\.;;' RELEASE)

dist_tag: RELEASE
	$(shell echo "ystorm_`cat RELEASE`" | sed 's/\./_/g' > DIST_TAG)

#TODO need a good way to see if these values would have changed and update only then
BASE_VERSION: ../pom.xml
	cd .. && $(MVN) help:evaluate -Dexpression=project.version | grep -v "^\[" | tail -1 > yahoo-build/BASE_VERSION

BASE_VERSION_UNDERSCORE: BASE_VERSION
	echo "$(BASE_VERSION)" | sed -e 's/-/_/g' > BASE_VERSION_UNDERSCORE

RELEASE: BASE_VERSION_UNDERSCORE
	@ if [ "$(YSTORM_BUILD_NUMBER)" != "" ]; then \
		echo "$(BASE_VERSION_UNDERSCORE).$(YSTORM_BUILD_NUMBER)" > RELEASE; \
	else \
		/home/y/bin/auto_increment_version.pl ystorm $(BASE_VERSION_UNDERSCORE) > RELEASE; \
	fi

define setup-versions
cd .. && ${MVN} -q -Pyahoo,storm-core,storm-more versions:set -DnewVersion=$(MVN_FULL_VERSION) ${MVN_MAKE_OPTS} && \
	${MVN} -q -Pyahoo,storm-core,storm-more versions:update-child-modules ${MVN_MAKE_OPTS}
if [ -e ../storm-core/dependency-reduced-pom.xml ]; then sed -e 's/'"$(BASE_VERSION)"'/'"$(FULL_VERSION)"'/g' ../storm-core/dependency-reduced-pom.xml > storm-core-dependency-reduced-pom.xml; fi
endef

define revert-versions
cd .. && ${MVN} -q -Pyahoo,storm-core,storm-more versions:revert ${MVN_MAKE_OPTS} && \
	${MVN} -q -Pyahoo,storm-core,storm-more versions:update-child-modules ${MVN_MAKE_OPTS}
endef

define find-tracked-pom-xml
git ls-files 'pom.xml' '**/pom.xml'
endef

testpkg: RELEASE BASE_VERSION build
	$(setup-versions)
	yinst_create --buildtype test ystorm.yicf && $(revert-versions) || ( $(revert-versions) && false )

ignore-pom-changes:
	cd .. && $(find-tracked-pom-xml) | xargs git update-index --assume-unchanged
	
# This target is called from screwdriver build and leaves the poms changed for its
# publish phase
package-sd: RELEASE build
	$(setup-versions)
	cd .. && $(find-tracked-pom-xml)| xargs git update-index --assume-unchanged
	yinst_create --buildtype release ystorm.yicf
	cd .. && $(find-tracked-pom-xml) | xargs git update-index --no-assume-unchanged

package: package-sd
	$(revert-versions)


rhel6_build.state: rhel6_build.yidf rhel6_build.yml
	rm -f rhel6_build.state rhel6_build.state.tmp rhel6_build.ruleset
	statecc --branch test ./rhel6_build.yml

clean:
	cd .. && $(MVN) -Pyahoo,storm-core,storm-more clean ${MVN_MAKE_OPTS}
	$(revert-versions)
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
