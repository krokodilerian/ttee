

// amount of slots in the ring buffer
#define MAXBUF 64

// maximum amount of query blocks in slot
#define MAXQ 50

// maximum number of output files
#define MAXFILES 64

// maximum size of a packet received
// anything more than this can be considered DoS
#define BUFSZ 8*1024

// if we don't fill the buffer in this interval, we write it anyway
#define DEADTIME 5

// change this to enable debugging
#undef DEBUG

// Display stats every STATS_INTERVAL seconds
#define STATS_INTERVAL 5

