#!/usr/bin/env bash

VERSION=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "systemslab-server") | .version')
RELEASE=${RELEASE:-1}

cat <<EOM
 ($VERSION-$RELEASE) $(lsb_release -sc); urgency=medium

  * Automated update package for rpc-perf $VERSION

 -- IOP Systems <sean@iop.systems>  $(date -R)
EOM