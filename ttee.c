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
	char *cbuf; // buffer with data
	int buflen;
	int bufpos;
	pthread_mutex_t wrlock;

	int fd;
	int fdo[MAXFILES];

	int dropped, processed;
	int numfiles;

};

struct threaddat {
	struct wrdata *dat;
	int fdno;
};



#ifndef max
	#define max( a, b ) ( ((a) > (b)) ? (a) : (b) )
#endif

#ifndef min
	#define min( a, b ) ( ((a) < (b)) ? (a) : (b) )
#endif


int diffpos(int a, int b) {
	if (b>=a) return b-a;
	return a-b + BUFFER;
}

/*
	check if we read a packet at position rp in a ring buffer
	with size of BUFFER, we won't clobber wp
*/
int overflows (int rp, int len, int wp) {
	int p1, p2;

	if ( ( (rp+len) < BUFFER ) && ( (wp <= rp) || (wp > rp + len) ) )
		return 0;

	// really fugly corner case
	if ( (rp+len) == BUFFER && wp == 0 )
		return 1;

	p1 = BUFFER - rp;
	p2 = len - p1;

	if ( (wp < rp) && (wp > p2) )
		return 0;

	return 1;
}


#define RP dat->rpos
#define WP dat->wpos

void *rcv_thread(void *ptr) {
	char buff[BUFSZ];
	struct wrdata *dat = (struct wrdata *) ptr;
	int buffdat = 0;

	int j;
	int p1, p2;

	struct timeval tv;

	struct timespec tv2;

	fd_set reads;
	long flags;	

	flags = fcntl( dat->fd, F_GETFL, 0 );
	fcntl( dat->fd, F_SETFL, flags | O_NONBLOCK );

	dat->cbuf = calloc( BUFFER, 1 );
	RP = 0;

	while (42) {

		tv.tv_sec = 1;
		tv.tv_usec = 0;

		FD_ZERO( &reads );
		FD_SET( dat->fd, &reads );

		// we check nothing. There are no error conditions that we care about.
		select( dat->fd + 1, &reads, NULL, NULL, &tv );

		if ( (buffdat = read ( dat->fd, buff, BUFSZ ) ) <=0 )
			continue;

		pthread_mutex_lock(&dat->wrlock);


		// the current buffer is full, switch to a new one
		// or the timeout passed
		for (j=0; j<dat->numfiles; j++) 
			if ( overflows (RP,  buffdat, WP[j]) ) {
				DBG("Ring buffer overflow, sleeping - RP %d WP %d WPid %d\n", RP, WP[j], j);
				pthread_mutex_unlock( &dat->wrlock );
				tv2.tv_sec = 0;
				tv2.tv_nsec = 1000*1000*10; // 10ms
				nanosleep( &tv2, NULL );
				continue;
		}

		if (RP + buffdat <= BUFFER) {
			// easy case
			memcpy ( &(dat->cbuf[RP]), buff, buffdat ); 
		} else {
			// fuck
			p1 = BUFFER - RP;
			p2 = buffdat - p1;

			memcpy ( &(dat->cbuf[RP]), buff, p1 ); 
			memcpy ( &(dat->cbuf[0]), &buff[p1], p2 ); 

		}
		RP += buffdat;
		RP %= BUFFER;
		DBG ("RP moved to %d\n", RP);

		pthread_mutex_unlock( &dat->wrlock );
		dat->processed += buffdat;
	}
	return NULL;
}


void *wrt_thread(void *ptr) {
	struct threaddat *tdat = (struct threaddat *) ptr;
	struct wrdata *dat = (struct wrdata *) tdat->dat;
	char *wrbuf;
	struct timespec tv;

	int fdno = tdat->fdno;

	int datalen;

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
			if (RP > WP[fdno]) {
				datalen = RP - WP[fdno];
			} else {
				datalen = BUFFER - WP[fdno];
			}
			wrbuf = malloc(datalen);
			memcpy(wrbuf, &dat->cbuf[WP[fdno]], datalen);

			WP[fdno] += datalen;
			WP[fdno] %= BUFFER;

			pthread_mutex_unlock( &dat->wrlock );// Dumping the data, leave the reader to work	

			// Write the data 

			DBG("thread %d really writing %d %d\n", fdno, WP[fdno], datalen);
			if ( write( dat->fdo[fdno], wrbuf, datalen ) < datalen ) {
				DBG("failed writing in thread %d, error %s\n", fdno, strerror(errno));
				exit(3);
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

	dat->numfiles = num;

	pthread_mutex_init(&dat->wrlock, NULL);

	dat->fd = STDIN_FILENO;

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

	setlinebuf(stdout);
	setlinebuf(stderr);

	if (argc < 2 || argc > MAXFILES ) {
		printf ("Usage: %s file1 file2 [file3] [file4]...[file64]\n", argv[0]);
		exit(3);
	}

	data = init_wrdata (argc - 1, &argv[1]);

	if (!data)
		exit(4);
		
	pthread_attr_init(&attr);
	pthread_create(&thread, &attr, rcv_thread, data);

	for (i=0; i<data->numfiles; i++) {
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
		for (i=0; i<data->numfiles; i++) 
			printf (" %d", diffpos(data->rpos, data->wpos[i]));
		printf("\n");
	}

	return 0;
}
