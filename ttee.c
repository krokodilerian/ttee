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

	Simple threaded 'tee' replacement.

	The basic idea is to use one reader and multiple writer threads
	that share a ring buffer containing the data. 

*/

#ifdef DEBUG
  #define DBG(x...) fprintf(stderr,## x)
#else
  #define DBG(x...)
#endif

struct wrdata {
	int rpos; // read thread pointer
	int wpos[MAXFILES]; // write thread pointer
	char *cbuf; // buffer with data
	pthread_mutex_t wrlock;

	int fd;
	int fdo[MAXFILES];

	int dropped, processed;
	int numfiles;

	int eof;

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

inline void sleep_10ms() {
	struct timespec tv;
	tv.tv_sec = 0;
	tv.tv_nsec = 1000*1000*10; // 10ms
	nanosleep( &tv, NULL );
}

/*
	returns the distance between positions a and b in a
	ringbuffer with size of BUFFER
*/
int diffpos(int a, int b) {
	if (b>=a) return b-a;
	return a-b + BUFFER;
}

/*
	check wether if we read a packet at position rp in a ring buffer
	with size of BUFFER, we won't clobber wp

	it's basically like this:

	if we don't overflow the buffer, then wp needs to be either before
	rp, or after rp+len

	if we overflow the buffer, then wp needs to be before rp AND after
	the remainder of the piece of data (len + rp - BUFFER)

	if we are exactly at the end of the buffer and the wp is 0, we 
	have a fucking specific corner case.

*/
int overflows (int rp, int len, int wp) {
	int rem;

	if (wp == rp) return 0;

	if ( ( (rp+len) < BUFFER ) && ( (wp < rp) || (wp > rp + len) ) )
		return 0;

	// really fugly corner case
	if ( (rp+len) % BUFFER == wp )
		return 1;

	rem = len + rp - BUFFER;

	if ( (wp < rp) && (wp > rem) )
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

	fd_set reads;
	long flags;	

	flags = fcntl( dat->fd, F_GETFL, 0 );
	fcntl( dat->fd, F_SETFL, flags | O_NONBLOCK );

	dat->cbuf = calloc( 1, BUFFER );

	while (42) {

		tv.tv_sec = 1;
		tv.tv_usec = 0;

		FD_ZERO( &reads );
		FD_SET( dat->fd, &reads );

		// we check nothing. There are no error conditions that we care about.
		select( dat->fd + 1, &reads, NULL, NULL, &tv );

		if ( (buffdat = read ( dat->fd, buff, BUFSZ ) ) <=0 ) {
			if ( (buffdat == -1) && (errno = EAGAIN))
				continue;
			__sync_fetch_and_add(&dat->eof, 1);
			pthread_exit(NULL);
		}

		pthread_mutex_lock(&dat->wrlock);

		// the current buffer is full, switch to a new one
		// or the timeout passed
		for (j=0; j<dat->numfiles; j++) 
			if ( overflows (RP,  buffdat, WP[j]) ) {
				DBG("Ring buffer overflow, sleeping - RP %d WP %d WPid %d\n", RP, WP[j], j);
				pthread_mutex_unlock( &dat->wrlock );
				sleep_10ms();
				continue;
		}

		if (RP + buffdat <= BUFFER) {
			// easy case
			memcpy ( &(dat->cbuf[RP]), buff, buffdat ); 
		} else {
			/* not-so-easy-case
			   split the bufer in two, and copy them separately
			*/
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

	int datalen;

	int fdno = tdat->fdno;


	DBG("Starting write thread %d\n", fdno);	

	while (42) {

		if ( RP == WP[fdno] ) {

			if (dat->eof > 0) {
				__sync_fetch_and_add(&dat->eof, 1);
				if (dat->eof == (dat->numfiles + 1) )
					exit(0);
				pthread_exit(NULL);
			}

			sleep_10ms();
			continue;
		}

		for ( ; WP[fdno] != RP; ) {
			pthread_mutex_lock( &dat->wrlock );
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

			DBG("thread %d writing %d %d\n", fdno, WP[fdno], datalen);
			if ( write( dat->fdo[fdno], wrbuf, datalen ) < datalen ) {
				DBG("failed writing in thread %d, error %s\n", fdno, strerror(errno));
				exit(3);
			}

			free( wrbuf );
		}
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
			printf("Failed opening %s with error %s\n", outpath[i], strerror(errno));
			return NULL;
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
		printf("Procesed:\t%d\tWP %d RP", data->processed, data->rpos);
		for (i=0; i<data->numfiles; i++) 
			printf (" %d", diffpos(data->rpos, data->wpos[i]));
		printf("\n");
	}

	return 0;
}
