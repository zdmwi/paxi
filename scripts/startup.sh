#! /usr/bin/env bash

sudo apt-get -yq update
echo "Installing golang"
curl -O https://dl.google.com/go/go1.22.3.linux-amd64.tar.gz 
tar -C . -xzf go1.22.3.linux-amd64.tar.gz

echo "export PATH=${PATH}:~/go/bin" >> ~/.bash_profile
