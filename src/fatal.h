#ifndef CSVDB_FATAL_H
#define CSVDB_FATAL_H

#include <setjmp.h>

/**
 * Query-time abort. In batch mode this is exit(code). While the REPL has
 * installed a catch via csvdb_fatal_catch(), it longjmps back instead so the
 * session can continue after an error that would otherwise kill the process.
 * Marked noreturn: callers never continue after this call.
 */
void csvdb_fatal(int code) __attribute__((noreturn));

/**
 * Install (non-NULL) or clear (NULL) the REPL recovery jump target.
 * Must be paired with setjmp(*buf) before running a query.
 */
void csvdb_fatal_catch(jmp_buf *buf);

#endif
