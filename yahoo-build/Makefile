#This is for updateing the state file.  I hate that we have to have --no-validate, but there are issues with svnlite/ysvn :(
rhel4_build.state: rhel4_build.yidf  rhel4_build.yml
	rm -f rhel4_build.state rhel4_build.ruleset
	statecc --branch test --no-validate ./rhel4_build.yml

clean:
	rm -rf rhel4_build.ruleset
