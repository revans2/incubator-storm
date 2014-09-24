include /home/y/share/yahoo_cfg/screwdriver/Make.rules

export GIT_REPO = git@git.corp.yahoo.com:storm/storm_tools.git
export GIT_SCRIPTS_BRANCH = storm_tools
export STORM_LATEST_RELEASE_TAG = ystorm_master_launcher_latest
export STORM_MASTER_PKGS = ystorm
export AUTO_CREATE_RELEASE_TAG = 1
export UPDATE_DIST_TAG_WITH_MASTER_PKG = 1

internal:
	git clone ${GIT_REPO} internal
	(cd internal && git checkout ${GIT_SCRIPTS_BRANCH})

copy_test_files:
	for dir in ${SRC_DIR}/storm-core/target/test-reports   ${SRC_DIR}/storm-core/target/surefire-reports  ${SRC_DIR}/storm-yahoo/target/surefire-reports  ${SRC_DIR}/examples/storm-starter/target/surefire-reports  ${SRC_DIR}/external/storm-kafka/target/surefire-reports ; do \
		if [ -d $$dir ] ;\
		then \
			cp $$dir/*.xml ${TEST_RESULTS_DIR} ;\
		fi ;\
	done

screwdriver: internal 
	$(MAKE) -C yahoo-build component_clean build test package-sd ; if [ $$? -eq 0 ] ; then $(MAKE) copy_test_files ;  else $(MAKE) copy_test_files; false ; fi
