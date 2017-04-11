#!/bin/bash

# These output files will be created by running the script:
DOT=module_deps.dot
PNG=module_deps.png

# Return a list of module dependencies based on external calls.
function deps() {
    # Discard comment lines, get rid of inline comments, grep module names
    # from external calls, lose the colon, do a unique sort.
    cat ../src/$1.erl | \
        grep -v "^[[:space:]]*%" | \
        sed 's|%.*$||g' | \
        grep -Eo "ds[a-z_]*:" | \
        cut -d ':' -f 1 | \
        sort | \
        uniq
}

cat <<EOF > $DOT
digraph ModuleDeps {
    rankdir = TD;

EOF

for f in ../src/*.erl ; do
    m=$(echo $f | sed 's|^../src/||g; s|.erl$||g');
    for d in $(deps $m) ; do
        echo "    $m -> $d" >> $DOT
    done
done
echo "}" >> $DOT

dot -Tpng -o $PNG $DOT
