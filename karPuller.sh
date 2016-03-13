#!/bin/bash
rm -rf ./kars
mkdir ./kars
for d in ./*/; do
	( cd $d && mvn clean install -Dmaven.test.skip=true && find -name *.kar -exec cp {} ../kars \; )
done