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

screwdriver: internal 
	$(MAKE) -C yahoo-build component_clean build test package-sd
