#!/bin/bash
set -e
. /var/lib/jenkins/jobs/master.get_branch_repo/workspace/big-data/pack-funcs

productName=elasticsearch

# 1) Check if the version is changed or not. If not changed, dont create a new debian.
checkVersion $productName "plugin"
# 2) Get the sources which are downloaded from version control system
#    to local machine to relevant directories to generate the debian package
getSourcesToRelevantDirectories $productName "plugin"
# 3) Create the Debian package
generateDebianPackagePlugins $productName
# 4) Create the Wrapper Repo Debian Package
generateRepoPackage $productName-"subutai-plugin"
