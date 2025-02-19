#ifdef CSVDB_VERSION
#define _STR(X) #X
#define STR(X) _STR(X)
const char *gitversion = STR(CSVDB_VERSION);
#else
extern const char *gitversion;
#endif