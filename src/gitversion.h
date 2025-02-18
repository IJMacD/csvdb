#ifdef CSVDB_VERSION
#define STR(X) #X
const char *gitversion = STR(CSVDB_VERSION);
#else
extern const char *gitversion;
#endif