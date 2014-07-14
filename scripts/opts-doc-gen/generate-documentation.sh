#!/usr/bin/env bash

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASEDIR=$SCRIPTDIR/../..

MONO_PATH=/opt/mono/bin/mono
DOCUMENTATIONOUTPUT_PATH=$BASEDIR/tools/documentation-generation/documentation.md
DOCUMENTATIONGEN_PATH=$BASEDIR/tools/documentation-generation/EventStore.Documentation.exe

EVENTSTORESINGLENODE_PATH=$BASEDIR/bin/singlenode
EVENTSTORECLUSTERNODE_PATH=$BASEDIR/bin/clusternode

$MONO_PATH $DOCUMENTATIONGEN_PATH -b:EVENTSTORESINGLENODE_PATH -b:EVENTSTORECLUSTERNODE_PATH -o:DOCUMENTATIONOUTPUT_PATH
