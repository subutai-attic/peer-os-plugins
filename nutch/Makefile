#!/usr/bin/make -f

PACKAGE_NAME=nutch-subutai-plugin
BUILD_DIR=$(PACKAGE_NAME)-debian-build
DEBIAN_BUILD_DIR=debian/$(PACKAGE_NAME)

all: package


clean:
	@rm -rf $(BUILD_DIR)
	@rm -rf $(DEBIAN_BUILD_DIR)

package:
	@debian/sync_versions
	@mvn clean install
	@dpkg-buildpackage -us -uc -rfakeroot 2>/dev/null
	@if [ ! -d ${BUILD_DIR} ]; then mkdir ${BUILD_DIR}; fi
	@mv ../*$(PACKAGE_NAME)_* ${BUILD_DIR}
	@rm -rf $(DEBIAN_BUILD_DIR)
	@mv ${BUILD_DIR}/*.deb target/

install:
	@echo Installing nutch plugin's debian package
	dpkg -i ${BUILD_DIR}/*.deb

