#!/bin/bash

# Update debian
apt-get -y update
apt-get -y dist-upgrade

# Install dependencies
apt-get -y install mupdf-tools
