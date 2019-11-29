#!/bin/bash

if [ "$QUICK_INSTALL" == "true" ];
    then
    cd iudx-resource-server-interface
    if [ -e iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar ]
    then
    rm iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar
    fi
    cp target/iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar .
    
    java $JAVA_OPTIONS -jar iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar
else
    cd iudx-resource-server-interface
    mvn -T 1C clean
    mvn -T 1C package
    cp target/iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar .
    java $JAVA_OPTIONS -jar iudx-resource-server-interface-0.0.1-SNAPSHOT-fat.jar
fi
