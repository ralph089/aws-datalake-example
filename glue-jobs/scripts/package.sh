#!/bin/bash
# Package Glue jobs for deployment

set -e

echo "Packaging Glue jobs..."

# Create dist directory
mkdir -p ../../dist

# Export poetry dependencies
poetry export -f requirements.txt --output requirements.txt --without-hashes

# Create utils zip file
cd src
zip -r ../../../dist/utils.zip utils transformations validators -x "*/__pycache__/*" "*/.*"
cd ..

echo "Package created: dist/utils.zip"
echo "Requirements exported: requirements.txt"
echo "Ready for deployment!"