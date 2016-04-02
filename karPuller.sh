#!/bin/bash
rm -rf ./kars
mkdir ./kars
for d in ./*/; do
	( cd $d && find -name *.kar -exec cp {} ../kars \; )
done