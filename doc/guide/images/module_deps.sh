#!/bin/bash

# The script writes module dependency graph DOT to standard output.

# This is run from guide and NOT from guide/images
SRCPATH="../../src"

# Return a list of module dependencies based on external calls.
function deps() {
    # Discard comment lines, get rid of inline comments, grep module names
    # from external calls, lose the colon, do a unique sort.
    cat $SRCPATH/$1.erl | \
        grep -v "^[[:space:]]*%" | \
        sed 's|%.*$||g' | \
        grep -Eo "(^|[^[:alpha:]])ds[a-z_]*:" | \
        sed 's|^.ds|ds|g' | \
        cut -d ':' -f 1 | \
        sort | \
        uniq
}

cat <<EOF
digraph ModuleDeps {
    graph [ dpi = 75 ];
    rankdir = TD;
    node [fontname="NewCenturySchlbk-Roman"];

EOF

for f in $SRCPATH/*.erl ; do
    m=$(echo $f | sed "s|^$SRCPATH/||g; s|.erl$||g");
    for d in $(deps $m) ; do
        echo "    $m -> $d"
    done
done
echo "}"
