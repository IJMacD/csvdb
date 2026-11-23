#ifndef CSVDB_REPL_H
#define CSVDB_REPL_H

/**
 * Interactive REPL. `options` supplies the initial output flags (format /
 * headers), typically from CLI + TTY config defaults.
 */
void repl(int options);

#endif
