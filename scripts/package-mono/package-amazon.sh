#!/usr/bin/env bash

set -e

version=$1

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
MKBUNDLE_BIN=`which mkbundle`

function usage {
	echo <<EOF
Usage:
  $0 version

Note: This script assumes Mono is installed at the default locations from
      the Xamarin package files.
EOF
	exit
}

function writeLog {
	message=$1
	echo "[package-mono.sh] - $message"
}

if [[ "$version" == "" ]] ; then
	VERSIONSTRING="0.0.0.0"
	writeLog "Version defaulted to: 0.0.0.0"
else
	VERSIONSTRING=$version
	writeLog "Version set to: $VERSIONSTRING"
fi

MONOPREFIX="/usr/local"
CONFIGPREFIX="/usr/local/etc"

if [[ -f $MKBUNDLE_BIN ]] ; then
	writeLog "Using mkbundle: $MKBUNDLE_BIN"
else
	writeLog "Cannot find mkbundle"
	exit 1
fi

MACHINECONFIG=$CONFIGPREFIX/mono/4.0/machine.config
if [[ -f $MACHINECONFIG ]] ; then
    writeLog "Using --machine-config: $MACHINECONFIG"
else
    writeLog "Cannot find machine config at $MACHINECONFIG"
    exit 1
fi

MONOCONFIG=$CONFIGPREFIX/mono/config
if [[ -f $MONOCONFIG ]] ; then
    writeLog "Using --config: $MONOCONFIG"
else
    writeLog "Cannot find mono config at $MONOCONFIG"
    exit 1
fi

GCCPATH=`which gcc`
if [[ $? != 0 ]] ; then
	writeLog "Cannot find gcc"
	exit 1
else
	writeLog "Using gcc: $GCCPATH"
fi

OUTPUTDIR="$SCRIPTDIR/../../bin/packaged"
[[ -d $OUTPUTDIR ]] || mkdir -p $OUTPUTDIR

soext="so"
PACKAGENAME="EventStore-OSS-Amazon2015.03-v$VERSIONSTRING"

PACKAGEDIRECTORY="$OUTPUTDIR/$PACKAGENAME"

if [[ -d $PACKAGEDIRECTORY ]] ; then
    rm -rf $PACKAGEDIRECTORY
fi
mkdir $PACKAGEDIRECTORY


pushd $SCRIPTDIR/../../bin/clusternode/

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:$MONOPREFIX/lib/pkgconfig

mkbundle -c -o clusternode.c -oo clusternode.a \
	EventStore.ClusterNode.exe \
	EventStore.Rags.dll \
	EventStore.Core.dll \
	EventStore.BufferManagement.dll \
	EventStore.Common.dll \
	EventStore.Projections.Core.dll \
	EventStore.ClusterNode.Web.dll \
	EventStore.Transport.Http.dll \
	EventStore.Transport.Tcp.dll \
	Newtonsoft.Json.dll \
	NLog.dll protobuf-net.dll \
	Mono.Security.dll \
	--static --deps --config $MONOCONFIG --machine-config $MACHINECONFIG

# mkbundle appears to be doing it wrong, though maybe there's something I'm not seeing.
sed -e '/_config_/ s/unsigned //' -i"" clusternode.c

# Forcibly set MONO_GC_DEBUG=clear-at-gc unless it's set to something else
sed -e 's/mono_mkbundle_init();/setenv("MONO_GC_DEBUG", "clear-at-gc", 0);\
        mono_mkbundle_init();/' -i"" clusternode.c

cc -o eventstored \
	-Wall `pkg-config --cflags monosgen-2` \
	clusternode.c \
	`pkg-config --libs-only-L monosgen-2` \
	-Wl,-Bstatic -lmonosgen-2.0 \
	-Wl,-Bdynamic `pkg-config --libs-only-l monosgen-2 | sed -e "s/\-lmonosgen-2.0 //"` \
	clusternode.a

cp -r clusternode-web $PACKAGEDIRECTORY/
cp -r Prelude $PACKAGEDIRECTORY/
cp -r projections $PACKAGEDIRECTORY/
cp libjs1.$soext $PACKAGEDIRECTORY/
cp eventstored $PACKAGEDIRECTORY/
cp log.config $PACKAGEDIRECTORY/
cp $SCRIPTDIR/run-node.sh $PACKAGEDIRECTORY/run-node.sh

popd

pushd $SCRIPTDIR/../../bin/testclient

mkbundle -c \
	-o testclient.c \
	-oo testclient.a \
	EventStore.TestClient.exe \
	EventStore.Core.dll \
	EventStore.Rags.dll \
	EventStore.ClientAPI.dll \
	EventStore.BufferManagement.dll \
	EventStore.Common.dll \
	EventStore.Transport.Http.dll \
	EventStore.Transport.Tcp.dll \
	Newtonsoft.Json.dll \
	NLog.dll \
	protobuf-net.dll \
	--static --deps --config $MONOCONFIG --machine-config $MACHINECONFIG

# mkbundle appears to be doing it wrong, though maybe there's something I'm not seeing.
sed -e '/_config_/ s/unsigned //' -i"" testclient.c

# Forcibly set MONO_GC_DEBUG=clear-at-gc unless it's set to something else
sed -e 's/mono_mkbundle_init();/setenv("MONO_GC_DEBUG", "clear-at-gc", 0);\
        mono_mkbundle_init();/' -i"" testclient.c

cc -o testclient \
	-Wall `pkg-config --cflags monosgen-2` \
	testclient.c \
	`pkg-config --libs-only-L monosgen-2` \
	-Wl,-Bstatic -lmonosgen-2.0 \
	-Wl,-Bdynamic `pkg-config --libs-only-l monosgen-2 | sed -e "s/\-lmonosgen-2.0 //"` \
	testclient.a

cp testclient $PACKAGEDIRECTORY/

popd

pushd $OUTPUTDIR

tar -zcvf $PACKAGENAME.tar.gz $PACKAGENAME
rm -r $PACKAGEDIRECTORY

[[ -d ../../packages ]] || mkdir -p ../../packages
mv $PACKAGENAME.tar.gz ../../packages/

popd

rm -r $OUTPUTDIR

