#!/bin/sh -e

# Set library path for test binary
export LD_LIBRARY_PATH=./lib:$LD_LIBRARY_PATH

# Clean up any existing test databases
rm -f *.qmap *.qmap.* 2>/dev/null || true

# Run the test suite and compare output
./bin/test | diff expects.txt -

echo "Core tests passed!"

# Run extended test suite (stress tests, performance benchmarks)
./bin/test_extended

echo "All tests passed!"
