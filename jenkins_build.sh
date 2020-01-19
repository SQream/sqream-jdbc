#!/bin/bash -l
cd jdbc-driver
sed -i "6s|<version>.*</version>|<version>$version_num</version>|" pom.xml
l_mvn=`which mvn`
$l_mvn -f pom.xml package -DskipTests