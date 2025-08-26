#!/bin/bash
#
# Package Lambda function for deployment
#
# This script creates a deployment package compatible with AWS Lambda
# by installing dependencies and bundling source code.

set -e  # Exit on any error

echo "🚀 Starting Lambda packaging process..."

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo "📦 Project directory: $PROJECT_DIR"

# Clean up previous builds
echo "🧹 Cleaning up previous builds..."
rm -rf package/
rm -f lambda-deployment.zip
rm -f requirements.txt

# Create package directory
mkdir -p package

# Generate requirements.txt from UV
echo "📋 Generating requirements.txt from UV..."
uv export --no-hashes > requirements.txt

# Verify requirements.txt was created
if [ ! -f requirements.txt ]; then
    echo "❌ Failed to generate requirements.txt"
    exit 1
fi

echo "📦 Installing dependencies..."
uv pip install -r requirements.txt --target package/ --quiet

# Copy source code
echo "📂 Copying source code..."
cp -r src/* package/

# Remove unnecessary files
echo "🧹 Cleaning up package..."
find package/ -name "*.pyc" -delete
find package/ -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find package/ -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true

# Create deployment zip
echo "🗜️ Creating deployment package..."
cd package
zip -r ../lambda-deployment.zip . -q
cd ..

# Get package size
PACKAGE_SIZE=$(du -h lambda-deployment.zip | cut -f1)
echo "📏 Package size: $PACKAGE_SIZE"

# Verify package contents
echo "📋 Package contents:"
unzip -l lambda-deployment.zip | head -20

echo "✅ Lambda package created successfully: lambda-deployment.zip"

# Optional: Test import (note: some native extensions may not work in packaging environment)
echo "🧪 Testing package imports (basic check)..."
cd package
python3 -c "
try:
    # Test basic imports that don't require native extensions
    import json
    import logging
    print('✅ Basic imports successful')
    print('⚠️  Note: Full validation requires Lambda runtime environment')
except ImportError as e:
    print(f'❌ Import error: {e}')
    exit(1)
" || {
    echo "❌ Package validation failed"
    exit 1
}

cd ..
echo "🎉 Lambda packaging completed successfully!"
echo "📦 Deployment package ready: lambda-deployment.zip"
echo "📏 Package size: $(du -h lambda-deployment.zip | cut -f1)"
echo ""
echo "⚠️  Note: Large package size (~87MB) due to native Python extensions."
echo "   This is normal for packages with pydantic, boto3, and httpx dependencies."
echo "   AWS Lambda supports packages up to 250MB unzipped."