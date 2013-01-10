BASE_VERSION:=`head -1 ../project.clj | awk '{print $$3}' | sed -e 's/\"//' | sed -e 's/\"//'`
BUILD_NUMBER?=0
FULL_VERSION="$(BASE_VERSION).$(BUILD_NUMBER)"
LEIN=LD_LIBRARY_PATH=/home/y/lib64 /home/y/bin/lein

all: build

test: build
	cd .. && $(LEIN) test

build:
	cd .. && $(LEIN) deps
	cd .. && $(LEIN) jar

#TODO need a good way to see if these values would have changed and update only then
BASE_VERSION: ../project.clj
	echo $(BASE_VERSION) > BASE_VERSION

RELEASE:
	echo $(FULL_VERSION) > RELEASE

testpkg: RELEASE BASE_VERSION build
	yinst_create --buildtype test ystorm.yicf
	
package: RELEASE BASE_VERSION build
	yinst_create --buildtype release ystorm.yicf
	

#Ugly hack to remove svnlite from the state file because statcc cannot deal
# with y_subversion replacing svnlite
rhel4_build.state: rhel4_build.yidf  rhel4_build.yml
	rm -f rhel4_build.state rhel4_build.state.tmp rhel4_build.ruleset
	statecc --branch test --no-validate ./rhel4_build.yml
	grep -v svnlite rhel4_build.state > rhel4_build.state.tmp
	mv -f rhel4_build.state.tmp rhel4_build.state
	statecc-validate-state rhel4_build.state

clean:
	rm -rf rhel4_build.ruleset ystorm*.tgz ../*.jar ../lib ../classes BASE_VERSION RELEASE
	cd .. && $(LEIN) clean


#BuildyBlocks targets
BB_RULES=/home/y/share/yahoo_cfg/buildyblocks/Make.rules
include $(BB_RULES)

COMMIT_BIM=./commit.bim
commit: BB_Provision build test
commit: BB_BIM_FILE=$(COMMIT_BIM)

commit_clean: clean
