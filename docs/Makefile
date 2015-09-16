
.PHONY : default clean

default : oeis-tools.png oeis-tools.pdf

oeis-tools.png : oeis-tools.dot
	dot -Gdpi=120 -Tpng -o$@ $<

oeis-tools.pdf : oeis-tools.dot
	dot -Tpdf -o$@ $<
clean :
	$(RM) oeis-tools.png oeis-tools.pdf
