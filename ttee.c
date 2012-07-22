#define _GNU_SOURCE

#include <sys/socket.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>

#include <pwd.h>

#include "config.h"

/*

	Small daemon to buffer requests

	The principle of operation is as simple as possible - ring-buffer
	where every element is filled up by requests coming from an unix domain
	datagram socket. There are two threads - reader that reads from the
	socket, and a writer that flushes the data to somewhere (currently
	a mysql database, but it's easy to change). The daemon doesn't parse
	the data, just collects it and passes it.

	If the reader thread catches up with the writer, packets get dropped.

	If a buffer is not full, but more than DEADTIME seconds have passed,
	it's passed to the writer anyway. This will also happen with empty
	buffers if there's no data at all, but that's not something to be
	expected in normal operations.

	It's possible to calculate the maximum amount of memory this would take,
	it's something like MAXBUF*MAXQ*16*1024 (buffers times queries times 
	max size of a packet).

	This is also supposed to work with multiple read-write pairs on different
	sockets, to paralelize the different types of trafic.
*/

#ifdef DEBUG
  #define DBG(x...) fprintf(stderr,## x)
#else
  #define DBG(x...)
#endif

struct wrdata {
	int rpos; // position of the element receiving data
	int wpos[MAXFILES]; // position of the element being written
	char *cbuf[MAXBUF]; // buffers with data
	int numq[MAXBUF]; // number of queries
	int buflen[MAXBUF];
	int bufpos[MAXBUF];
	int bufref[MAXBUF];
	pthread_mutex_t wrlock;
	time_t firstq; // time of the first query for the current buffer

	int fd;
	int fdo[MAXFILES];
	char *prepend, *append;

	int dropped, processed;
};

struct threaddat {
	struct wrdata *dat;
	int fdno;
};


int numfiles;

#ifndef max
	#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif

#ifndef min
	#define min( a, b ) ( ((a) < (b)) ? (a) : (b) )
#endif


int diffpos(int a, int b) {
	if (b>=a) return b-a;
	return a-b + MAXBUF;
}


#define RP dat->rpos
#define WP dat->wpos

#define INIT_READ_BUFFER(DAT, NUM) { DAT->numq[NUM] = 0;\
	DAT->bufpos[NUM] = 0;\
	DAT->buflen[NUM] = BUFSZ;\
	DAT->cbuf[NUM] = calloc( BUFSZ, 1 );\
	DAT->firstq = time ( NULL );\
	DAT->bufref[NUM] = numfiles;\
}

void *rcv_thread(void *ptr) {
	char buff[BUFSZ];
	struct wrdata *dat = (struct wrdata *) ptr;
	int bufflen = BUFSZ;
	int buffdat = 0;

	int j;

	struct timeval tv;
	fd_set reads;
	long flags;	

	flags = fcntl( dat->fd, F_GETFL, 0 );
	fcntl( dat->fd, F_SETFL, flags | O_NONBLOCK );

	INIT_READ_BUFFER(dat, 0);

	while (42) {

		tv.tv_sec = 1;
		tv.tv_usec = 0;

		FD_ZERO( &reads );
		FD_SET( dat->fd, &reads );

		// we check nothing. There are no error conditions that we care about.
		select( dat->fd + 1, &reads, NULL, NULL, &tv );

		if ( time(NULL) - dat->firstq > DEADTIME ) {
			pthread_mutex_lock( &dat->wrlock );
			for (j=0; j<numfiles; j++) 
				if ( ( (RP + 1) % MAXBUF ) == WP[j] ) {
					// we either die here, because we're overflowed, or block for a while.
					// right now dying is easier.
					DBG("Ring buffer overflow, dying");
					exit(3);
					pthread_mutex_unlock( &dat->wrlock );
					// write pointer, we meet again
					// drop current data, loop.
					//DBG("Out of buffers %d %d!\n", RP, WP);
					// We need to read the packet to drop it.
					read ( dat->fd, buff, bufflen );
					dat->dropped ++;
					continue;
			}
			DBG("Timeout: New read buffer %d\n", RP);
			RP = (RP + 1) % MAXBUF;

			INIT_READ_BUFFER( dat, RP );

			pthread_mutex_unlock( &dat->wrlock );
		}

		if ( (buffdat = read ( dat->fd, buff, bufflen ) ) <=0 )
			continue;

		pthread_mutex_lock(&dat->wrlock);


		// the current buffer is full, switch to a new one
		// or the timeout passed
		if ( dat->numq[RP] >= MAXQ  || time( NULL ) - dat->firstq > DEADTIME ) {
			for (j=0; j<numfiles; j++) 
				if ( ( (RP + 1) % MAXBUF ) == WP[j] ) {
					// we either die here, because we're overflowed, or block for a while.
					// right now dying is easier.
					DBG("Ring buffer overflow, dying");
					exit(3);
					pthread_mutex_unlock( &dat->wrlock );
					// write pointer, we meet again
					// drop current data, loop.
					//DBG("Out of buffers %d %d!\n", RP, WP);
					// We need to read the packet to drop it.
					read ( dat->fd, buff, bufflen );
					dat->dropped ++;
					continue;
			}

			DBG("New read buffer %d oldsz %d\n", RP, dat->bufpos[RP]);
			RP = (RP + 1) % MAXBUF;

			INIT_READ_BUFFER( dat, RP );

		}

		// resize the buffer if needed
		if ( ( dat->buflen[RP] - dat->bufpos[RP] ) < buffdat ) {
			dat->buflen[RP] = dat->buflen[RP] + buffdat + 2 ;
			dat->cbuf[RP] = realloc ( dat->cbuf[RP], dat->buflen[RP] );
			
		}
		
		memcpy ( &(dat->cbuf[RP][dat->bufpos[RP]]), buff, buffdat ); 
		dat->bufpos[RP] += buffdat;
		dat->numq[RP] ++;
		pthread_mutex_unlock( &dat->wrlock );
		dat->processed ++;
	}
	return NULL;
}


void *wrt_thread(void *ptr) {
	struct threaddat *tdat = (struct threaddat *) ptr;
	struct wrdata *dat = (struct wrdata *) tdat->dat;
	char *wrbuf;
	struct timespec tv;

	int fdno = tdat->fdno;

	int datapos, numq;

	DBG("Starting write thread %d\n", fdno);	

	while (42) {

		if ( RP == WP[fdno] ) {

			tv.tv_sec = 0;
			tv.tv_nsec = 1000*1000*10; // 10ms
			nanosleep( &tv, NULL );

			continue;
		}

		pthread_mutex_lock( &dat->wrlock );
		for ( ; WP[fdno] != RP; ) {
			DBG("thread %d writing %d %d\n", fdno, WP[fdno], dat->bufpos[WP[fdno]]);
			if (dat->bufref[WP[fdno]] < 1) {
				DBG("FUCK %d %d %d %d\n", RP, WP[fdno], fdno, dat->bufref[WP[fdno]]);
				exit(2);
			}
			wrbuf = malloc(dat->bufpos[WP[fdno]]);
			memcpy(wrbuf, dat->cbuf[WP[fdno]], dat->bufpos[WP[fdno]]);
			__sync_fetch_and_sub(&dat->bufref[WP[fdno]],1);

			datapos = dat->bufpos[WP[fdno]];
			numq = dat->numq[WP[fdno]];

			// Can we dealloc this buffer?
			if (dat->bufref[WP[fdno]] == 0) {
				dat->bufref[WP[fdno]]=-1;
				free(dat->cbuf[WP[fdno]]);
				dat->numq[WP[fdno]] = 0;
				dat->cbuf[WP[fdno]] = NULL;
				DBG("thread %d dealloc %d\n", fdno, WP[fdno]);
			}

			WP[fdno] = ( WP[fdno] +1 ) % MAXBUF;

			pthread_mutex_unlock( &dat->wrlock );// Dumping the data, leave the reader to work	

			if ( numq == 0 ) { // empty/timed out buffer
				free( wrbuf );
				continue;
			}

			// Write the data 

			DBG("thread %d really writing %d %d\n", fdno, WP[fdno], datapos);
			if ( write( dat->fdo[fdno], wrbuf, datapos ) < datapos ) {
				DBG("failed writing in thread %d, error %s\n", fdno, strerror(errno));
				exit(3);
				dat->dropped += numq;
				free(wrbuf);

				break;
			}

			free( wrbuf );

			pthread_mutex_lock( &dat->wrlock );	
		}
		pthread_mutex_unlock( &dat->wrlock );	
	}
	return NULL;
}

#undef RP
#undef WP


struct wrdata *init_wrdata (int num, char **outpath) {
	int i;
	struct wrdata *dat;

	dat = (struct wrdata *) calloc (1, sizeof(struct wrdata));

	for (i = 0; i<MAXBUF ; i++) {
		dat->cbuf[i] = NULL;
		dat->bufref[i] = -1;
	}

	pthread_mutex_init(&dat->wrlock, NULL);

	dat->fd = 0;
	dat->firstq = time(NULL);
	for (i = 0; i<num; i++) {
		dat->fdo[i] = open(outpath[i], O_CREAT | O_WRONLY,  S_IRUSR | S_IWUSR );
		if (dat->fdo[i] == -1 ) {
			return NULL;
			printf("Failed opening %s with error %s\n", outpath[i], strerror(errno));
		}
	}

	return dat;
}


int main(int argc, char **argv) {

	struct timespec tv;
	struct wrdata *data;
	struct threaddat tdat[MAXFILES];

	int i;

	pthread_t thread;
	pthread_attr_t attr;

	if (argc < 2 || argc > MAXFILES ) {
		printf ("Usage: %s file1 file2 [file3] [file4]...[file64]\n", argv[0]);
		exit(3);
	}

	numfiles = argc - 1;
	data = init_wrdata (numfiles, &argv[1]);

	if (!data)
		exit(4);
		
	pthread_attr_init(&attr);
	pthread_create(&thread, &attr, rcv_thread, data);

	for (i=0; i<numfiles; i++) {
		tdat[i].dat = data;
		tdat[i].fdno = i;

		pthread_attr_init(&attr);
		pthread_create(&thread, &attr, wrt_thread, &tdat[i]);
	}

	while (42) {
		tv.tv_sec = STATS_INTERVAL;
		tv.tv_nsec = 0;
		nanosleep( &tv, NULL);
		printf("data: Dropped:\t%d\tProcesed:\t%d\tWP %d RP", data->dropped, data->processed, data->rpos);
		for (i=0; i<numfiles; i++) 
			printf (" %d", diffpos(data->rpos, data->wpos[i]));
		printf("\n");
	}

	return 0;
}
