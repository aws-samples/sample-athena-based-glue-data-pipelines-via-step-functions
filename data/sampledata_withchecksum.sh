set -euxo pipefail
#!/bin/bash
#script to download public source data can be used for testing after md5 validation
#make sure file verification passed else please dont use the file for testing.

#expected checksums
EXPECTED_IN_MD5="a2f3bc3e3cb99621a6b6f7d20440b6ec"
EXPECTED_OH_MD5="606b9521ff74bfa050501f007082446b"

# Detect operating system
OS=$(uname -s)
echo "Detected operating system: $OS"

# Function to calculate MD5 hash based on available commands
calculate_md5() {
  local md5_tool=""

  if command -v md5sum >/dev/null 2>&1; then
    echo "Using md5sum command" >&2
    md5_tool="md5sum | cut -d' ' -f1"
  elif [ "$OS" = "Darwin" ]; then
    echo "Using macOS md5 command" >&2
    md5_tool="md5 | sed 's/^.*= //'"
  elif command -v openssl >/dev/null 2>&1; then
    echo "Using openssl for MD5 calculation" >&2
    md5_tool="openssl md5 | sed 's/^.*= //'"
  elif command -v md5 >/dev/null 2>&1; then
    echo "Using md5 command" >&2
    md5_tool="md5 | sed 's/^.*= //'"
  else
    echo "No MD5 calculation tool found. Please install md5sum, openssl, or md5." >&2
    exit 1
  fi
  
  eval $md5_tool
}

# Function to download file only if MD5 matches
download_if_valid() {
  local url=$1
  local output_file=$2
  local expected_md5=$3
  
  # Get MD5 of the file without saving it
  local actual_md5=$(aws s3 cp "$url" --no-sign-request - | calculate_md5)
  echo "Calculated MD5: $actual_md5"
  echo "Expected MD5:   $expected_md5"
  
  # Check if MD5 matches
  if [ "$actual_md5" = "$expected_md5" ]; then
    echo "MD5 verification successful for $output_file"
    # Download the file
    aws s3 cp "$url" --no-sign-request "$output_file"
    return 0
  else
    echo "MD5 verification failed for $output_file - download aborted"
    return 1
  fi
}

# Download files only if MD5 matches
download_if_valid "s3://pudl.catalyst.coop/v2022.11.30/hourly_emissions_epacems/epacems-1996-IN.parquet" "epacems-1996-IN.parquet" "$EXPECTED_IN_MD5"
in_result=$?

download_if_valid "s3://pudl.catalyst.coop/v2022.11.30/hourly_emissions_epacems/epacems-1996-OH.parquet" "epacems-1996-OH.parquet" "$EXPECTED_OH_MD5"
oh_result=$?

# Final verification status
if [ $in_result -eq 0 ] && [ $oh_result -eq 0 ]; then
  echo "All files verified and downloaded successfully"
else
  echo "Some files failed verification and were not downloaded"
fi