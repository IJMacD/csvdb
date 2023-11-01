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
SRCS = $(RAWSRCS:src/%=%)
OBJS = $(SRCS:.c=.o)
EXE  = csvdb
GENEXE = gen
INSTALL_DIR = /usr/local/bin

#
# Debug build settings
#
DBGDIR = debug
DBGEXE = $(DBGDIR)/$(EXE)
DBGSRCS = $(SRCS) repl.c gitversion.c debug.c
DBGOBJS = $(addprefix $(DBGDIR)/, $(DBGSRCS:.c=.o))
DBGCFLAGS = -g -O0 -DDEBUG -DJSON_NULL -DJSON_BOOL

#
# Release build settings
#
RELDIR = release
RELEXE = $(RELDIR)/$(EXE)
RELSRCS = $(SRCS) repl.c gitversion.c
RELOBJS = $(addprefix $(RELDIR)/, $(RELSRCS:.c=.o))
RELCFLAGS = -O3 -DNDEBUG -DJSON_NULL -DJSON_BOOL

#
# CGI build settings
#
CGIDIR = release
CGIEXE = $(CGIDIR)/$(EXE).cgi
CGISRCS = $(filter-out main.c, $(SRCS)) main-cgi.c
CGIOBJS = $(addprefix $(CGIDIR)/, $(CGISRCS:.c=.o))

#
# CGI Debug build settings
#
CGIDDIR = debug
CGIDEXE = $(CGIDDIR)/$(EXE).cgi
CGIDSRCS = $(filter-out main.c, $(SRCS)) debug.c main-cgi.c
CGIDOBJS = $(addprefix $(CGIDDIR)/, $(CGIDSRCS:.c=.o))

#
# GEN build settings
#
GENDIR = release
GENEXE = $(GENDIR)/gen
GENSRCS = $(filter-out main.c, $(SRCS)) gen.c
GENOBJS = $(addprefix $(GENDIR)/, $(GENSRCS:.c=.o))

.PHONY: all clean debug prep release remake cgi test install

# Default build
all: prep release

#
# Debug rules
#
debug: prep $(DBGEXE)

$(DBGEXE): $(DBGOBJS)
	$(CC) $(CFLAGS) $(DBGCFLAGS) -o $(DBGEXE) $^

# Warning: overlaps with $(CGIDDIR)/%.o: $(SRCDIR)/%.c
$(DBGDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(DBGCFLAGS) -o $@ $<

#
# Release rules
#
release: prep $(RELEXE)

$(RELEXE): $(RELOBJS)
	$(CC) $(CFLAGS) $(RELCFLAGS) -o $(RELEXE) $^

$(RELDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(RELCFLAGS) -o $@ $<

#
# CGI rules
#
cgi: prep $(CGIEXE)

$(CGIEXE): $(CGIOBJS)
	$(CC) $(CFLAGS) -o $(CGIEXE) $^

# Warning: overlaps with $(RELDIR)/%.o: $(SRCDIR)/%.c
$(CGIDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(RELCFLAGS) -o $@ $<

#
# CGI DEBUG rules
#
cgi-debug: prep $(CGIDEXE)

$(CGIDEXE): $(CGIDOBJS)
	$(CC) $(CFLAGS) $(DBGCFLAGS) -o $(CGIDEXE) $^

# Warnging: overlaps with $(DBGDIR)/%.o: $(SRCDIR)/%.c
$(CGIDDIR)/%.o: $(SRCDIR)/%.c
	$(CC) -c $(CFLAGS) $(DBGCFLAGS) -o $@ $<

#
# Test rules
#
test: prep release test/test.csv
	cd test && ./test.sh

test/test.csv: $(GENEXE)
	${GENEXE} 1000000 $@

$(GENEXE): $(GENOBJS)
	$(CC) $(CFLAGS) $(RELCFLAGS) -o $@ $^

#
# Other rules
#
prep:
	@mkdir -p $(DBGDIR) $(addprefix $(DBGDIR)/, $(SUBDIRS)) $(RELDIR) $(addprefix $(RELDIR)/, $(SUBDIRS))

$(SRCDIR)/gitversion.c: .git/HEAD .git/index
	echo "const char *gitversion = \"$(shell git rev-parse HEAD)$(shell git diff-index --quiet HEAD && git show -s --format=' (%cd)' --date=iso-strict || (echo "-dirty built @" && date -Iseconds))\";" > $@

remake: clean all

clean:
	rm -f $(RELEXE) $(RELOBJS) $(DBGEXE) $(DBGOBJS) $(CGIEXE) ${CGIOBJS} ${GENOBJS} ${GENEXE} $(SRCDIR)/gitversion.c

install: release
	cp $(RELEXE) $(INSTALL_DIR)