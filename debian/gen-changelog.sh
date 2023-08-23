#!/usr/bin/env bash

VERSION=$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[] | select(.name == "rpc-perf") | .version')
RELEASE=${RELEASE:-1}

cat <<EOM
rpc-perf ($VERSION-$RELEASE) $(lsb_release -sc); urgency=medium

  * Automated update package for rpc-perf $VERSION

 -- IOP Systems <sean@iop.systems>  $(date -R)
EOM