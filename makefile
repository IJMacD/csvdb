#
# Compiler flags
#
CC     = gcc
CFLAGS = -Wall -Werror -Wextra -Wno-format-overflow

#
# Project files
#
SRCDIR = src
SUBDIRS = db query execute evaluate sort functions
SRCDIRS = $(addprefix $(SRCDIR)/, $(SUBDIRS))
RAWSRCS = main.c $(wildcard $(addsuffix /*.c, $(SRCDIRS)))
SRCS = $(RAWSRCS:src/%=%) repl.c gitversion.c
OBJS = $(SRCS:.c=.o)
EXE  = csvdb
GENEXE = gen
INSTALL_DIR = /usr/local/bin

#
# Debug build settings
#
DBGDIR = debug
DBGEXE = $(DBGDIR)/$(EXE)
DBGOBJS = $(addprefix $(DBGDIR)/, $(OBJS)) $(DBGDIR)/debug.o
DBGCFLAGS = -g -O0 -DDEBUG -DJSON_NULL

#
# Release build settings
#
RELDIR = release
RELEXE = $(RELDIR)/$(EXE)
RELOBJS = $(addprefix $(RELDIR)/, $(OBJS))
RELCFLAGS = -O3 -DNDEBUG -DJSON_NULL

#
# CGI build settings
#
CGIDIR = release
CGIEXE = $(CGIDIR)/$(EXE).cgi
CGISRCS = $(filter-out main.c, $(SRCS)) main-cgi.c
CGIOBJS = $(addprefix $(CGIDIR)/, $(CGISRCS:.c=.o))
CGICFLAGS = -O3 -DNDEBUG -DJSON_NULL

#
# GEN build settings
#
GENDIR = release
GENEXE = $(GENDIR)/gen
GENSRCS = $(filter-out main.c, $(SRCS)) gen.c
GENOBJS = $(addprefix $(GENDIR)/, $(GENSRCS:.c=.o))
GENCFLAGS = -O3 -DNDEBUG

.PHONY: all clean debug prep release remake cgi test install

# Default build
all: prep release

#
# Debug rules
#
debug: $(DBGEXE)

$(DBGEXE): $(DBGOBJS)
	$(CC) $(CFLAGS) $(DBGCFLAGS) -o $(DBGEXE) $^

$(DBGDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(DBGCFLAGS) -o $@ $<

#
# Release rules
#
release: $(RELEXE)

$(RELEXE): $(RELOBJS)
	$(CC) $(CFLAGS) $(RELCFLAGS) -o $(RELEXE) $^

$(RELDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(RELCFLAGS) -o $@ $<

#
# CGI rules
#
cgi: prep $(CGIEXE)

$(CGIEXE): $(CGIOBJS)
	$(CC) $(CFLAGS) $(CGICFLAGS) -o $(CGIEXE) $^

$(CGIDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(CGICFLAGS) -o $@ $<

#
# Test rules
#
test: prep release test.csv
	./test.sh

test.csv: $(GENEXE)
	${GENEXE} 1000000 test.csv

$(GENEXE): $(GENOBJS)
	$(CC) $(CFLAGS) $(GENCFLAGS) -o $@ $^

#
# Other rules
#
prep:
	@mkdir -p $(DBGDIR) $(addprefix $(DBGDIR)/, $(SRCDIRS)) $(RELDIR) $(addprefix $(RELDIR)/, $(SRCDIRS))

$(SRCDIR)/gitversion.c: .git/HEAD .git/index
	echo "const char *gitversion = \"$(shell git rev-parse HEAD)$(shell git diff-index --quiet HEAD || echo "-dirty") ($(shell git show -s --format=%cd --date=iso-strict))\";" > $@

remake: clean all

clean:
	rm -f $(RELEXE) $(RELOBJS) $(DBGEXE) $(DBGOBJS) $(CGIEXE) ${CGIOBJS} ${GENOBJS} ${GENEXE}

install: release
	cp $(RELEXE) $(INSTALL_DIR)