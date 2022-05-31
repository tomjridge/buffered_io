TMP_DOC_DIR:=/tmp/buffered_io
scratch:=/tmp

default: all

-include Makefile.ocaml

run:
	time $(DUNE) exec main

# for auto-completion of Makefile target
clean::
