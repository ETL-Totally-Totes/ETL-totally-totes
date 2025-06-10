#!/bin/bash

pip install --upgrade pip
echo "Cleaning up..."
rm -rf python 
rm -rf util_layer.zip
rm -rf layer.zip
echo "Creating dependencies"

pip install python-dotenv --platform manylinux2014_x86_64 --only-binary=:all: -t python/
zip -r layer.zip python
rm -rf python

# pip uninstall pyscopg2
pip install psycopg2-binary==2.9.9 --platform manylinux2014_x86_64 --only-binary=:all: -t python/
zip -r extract_extras_layer.zip python
rm -rf python

echo "Creating utils"
mkdir python
cp -r src python
zip -r util_layer.zip python
rm -rf python