#include <stdlib.h>
#include <setjmp.h>

#include "fatal.h"

static jmp_buf *fatal_jmp = NULL;

void csvdb_fatal_catch(jmp_buf *buf)
{
    fatal_jmp = buf;
}

void csvdb_fatal(int code)
{
    if (fatal_jmp != NULL)
    {
        /* longjmp cannot return 0; map that to 1. */
        longjmp(*fatal_jmp, code == 0 ? 1 : code);
    }

    exit(code);
}
