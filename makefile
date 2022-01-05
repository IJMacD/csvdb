#
# Compiler flags
#
CC     = gcc
CFLAGS = -Wall -Werror -Wextra -Wno-format-overflow

#
# Project files
#
SRCS = main.c db.c db-csv.c db-calendar.c db-csv-mem.c db-sequence.c query.c token.c parse.c predicates.c evaluate.c sort.c tree.c output.c create.c util.c explain.c indices.c plan.c function.c date.c result.c debug.c
SRCDIR = src
OBJS = $(SRCS:.c=.o)
EXE  = csvdb
GENEXE = gen

#
# Debug build settings
#
DBGDIR = debug
DBGEXE = $(DBGDIR)/$(EXE)
DBGOBJS = $(addprefix $(DBGDIR)/, $(OBJS))
DBGCFLAGS = -g -O0 -DDEBUG

#
# Release build settings
#
RELDIR = release
RELEXE = $(RELDIR)/$(EXE)
RELOBJS = $(addprefix $(RELDIR)/, $(OBJS))
RELCFLAGS = -O3 -DNDEBUG

#
# CGI build settings
#
CGIDIR = release
CGIEXE = $(CGIDIR)/$(EXE).cgi
CGISRCS = $(filter-out main.c, $(SRCS)) main-cgi.c
CGIOBJS = $(addprefix $(CGIDIR)/, $(CGISRCS:.c=.o))
CGICFLAGS = -O3 -DNDEBUG

.PHONY: all clean debug prep release remake cgi test

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
	./${GENEXE} 100000 test.csv

$(GENEXE): ./src/gen.c ./src/date.c
	$(CC) $(CFLAGS) $(DBGCFLAGS) -o $@ $^

#
# Other rules
#
prep:
	@mkdir -p $(DBGDIR) $(RELDIR)

remake: clean all

clean:
	rm -f $(RELEXE) $(RELOBJS) $(DBGEXE) $(DBGOBJS) $(CGIEXE) ${CGIOBJS}