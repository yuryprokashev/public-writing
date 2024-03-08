#!/bin/bash

# Set Variables from input
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <PYTHON_VERSION>"
    exit 1
fi
PYTHON_VERSION="$1"

# Check for installed packages
PACKAGES="libxml2-devel libxslt-devel gcc gcc-c++ make wget tar gzip"
NEEDED_PACKAGES=""
for pkg in $PACKAGES; do
    if ! rpm -q $pkg &> /dev/null; then
        NEEDED_PACKAGES="$NEEDED_PACKAGES $pkg"
    fi
done

# Install only needed packages
if [ -n "$NEEDED_PACKAGES" ]; then
    echo "Installing dependencies for lxml and TA-Lib python packages"
    yum install -y $NEEDED_PACKAGES
fi

# Download and unpack TA-Lib if not exists
if [ ! -f ta-lib-0.4.0-src.tar.gz ]; then
    echo "Downloading TA-Lib"
    wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
fi
echo "Unpacking TA-Lib"
tar -xzf ta-lib-0.4.0-src.tar.gz

# Guessing the hardware architecture so make util can build
echo "Updating config for architecture"
wget -O config.guess 'http://savannah.gnu.org/cgi-bin/viewcvs/*checkout*/config/config/config.guess' &
wget -O config.sub 'http://savannah.gnu.org/cgi-bin/viewcvs/*checkout*/config/config/config.sub' &
wait

# Install TA-Lib
echo "Installing TA-Lib"
cd ta-lib || exit

echo "Building TA-Lib"
PREFIX=/usr
./configure --prefix=$PREFIX
make -j4
make install
cd ..

echo "Copy libta_lib.so.* files to /asset-output/lib so lambda layer will expose it to lambda from /opt/lib" && \
mkdir -p /asset-output/lib && \
cp $PREFIX/lib/libta_lib.so* /asset-output/lib/ && \

echo "Setting env variables for TA-Lib python package to build" && \
export TA_LIBRARY_PATH=$PREFIX/lib && \
export TA_INCLUDE_PATH=$PREFIX/include && \

echo "Installing python dependencies including TA-Lib" && \
mkdir -p /asset-output/python/lib/python"$PYTHON_VERSION"/site-packages/function/layer && \
pip install -r /asset-input/requirements.txt -t /asset-output/python/lib/python"$PYTHON_VERSION"/site-packages
