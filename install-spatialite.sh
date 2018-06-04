#!/bin/bash

set -e

LIBSPATIALITE="libspatialite-4.3.0a"

if [[ ! -d ${LIBSPATIALITE}/src/.libs ]]; then

    wget http://www.gaia-gis.it/gaia-sins/${LIBSPATIALITE}.tar.gz
    tar xvzf ${LIBSPATIALITE}.tar.gz

    cd ${LIBSPATIALITE}

    ./configure --disable-freexl --disable-libxml2
    make -j2
else
    cd ${LIBSPATIALITE}
fi

sudo make install
