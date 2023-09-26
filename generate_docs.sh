#!/bin/bash

# Default configuration options
GENERATE_WHEEL=false  # Default to not generate the wheel file

# Process command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --wheel)
            GENERATE_WHEEL=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Ensure pydoc-markdown is installed
if ! command -v pydoc-markdown &> /dev/null; then
    echo "pydoc-markdown is not installed. Please install it first."
    return 1
fi

# Add the current directory to PYTHONPATH if not already added
if [[ ! ":$PYTHONPATH:" == *":$PWD:"* ]]; then
    export PYTHONPATH=$PWD:$PYTHONPATH
fi

# Generate the Markdown file using pydoc-markdown
pydoc-markdown -m historian_query '{
    renderer: {
      type: markdown,
      descriptive_class_title: false,
      render_toc: false,
      insert_header_anchors: false
    }
  }' > README.md

# Process each Markdown file in the current directory
for file in *.md; do
    # Fix MD001 heading increment jump from h2 to h4 by raising to h3
    sed -i 's/####/###/g' "$file"
    # Fix MD002 double line break at the end of file
    sed -i ':a;${/^\n*$/{$d;N;};/\n$/ba}' "$file"
done

echo "Markdown files processed successfully."

# Generate the wheel file if the --wheel flag is specified
if [ "$GENERATE_WHEEL" = true ]; then
    pytest tests/
    python setup.py bdist_wheel
    check-wheel-contents dist/
    echo "Wheel file generated successfully."
fi