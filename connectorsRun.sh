#!/usr/bin/bash
ACT_PATH=`pwd`
CONNECTORS_PATH='/volume1/web/teamData/connectors/'
cd $CONNECTORS_PATH
node runConnectors.js >> ../log/connectorsRun.log
cd $ACT_PATH
