GEN_DOT = images/module_deps.dot

GEN_PNG = images/module_deps.png \
          images/spec_tree.png \
          images/type_hierarchy.png

PNG = images/dumpsterl_gui.png $(GEN_PNG)

all: book.html

book.html: book.adoc $(PNG)
	asciidoctor book.adoc

$(GEN_PNG):%.png: %.dot
	dot -Tpng -o $@ $^

images/module_deps.dot: images/module_deps.sh
	images/module_deps.sh > $@

clean:
	rm -f book.html $(GEN_DOT) $(GEN_PNG)
