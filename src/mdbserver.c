/* -------------------------------------------------------------------------- *
 * Description : miniDB server
 *
 * ------------------------------------------------------------------------- *
 * Copyright (c) 2010 Hans Harder
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * ------------------------------------------------------------------------- */
#include <stdio.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <asm/poll.h>
#include "bt.h"
/* Translation--------------------------------------------- */
typedef struct {
    eAdrType  recptr;
    char 	    key[80];
} ENTRY;
#define IX_DESC btFP
#define IX_OK	bOk
#define EOIX	bErrEndOfIndex
#define clear_key(e)  memset(e,0,sizeof(e))
/* -------------------------------------------------------- */
#include "mdb_lib.h"

int 	NOROWS=1;		/* no of rows to retrieve */
int 	LOGGING=0;		/* no logging to stdout */


struct tcp_ses {
    int 	 socket_fd;
    char	 user[21];
    char	 ipadr[21];
    time_t timeout;
};

#define WAKEUPTM 6000			/* wakeup time */
#define MAXIDLE  600			/* max 10 min idletime */
#define MAXCONN  200			/* max 200 connections */

struct  pollfd   pollfds[MAXCONN];
struct  tcp_ses *session[MAXCONN];
int 	contot = 0;			/* total number of connections */
int	netsocket;


syntax() {
    puts("Syntax : mdbserver [-d <directory>] [-l] [-p <portno>]");
    puts("         miniDB server daemon");
    puts("");
    puts("Options: -d  database directory");
    puts("         -l  log commands to stdout");
    printf("         -p  portno to listen for connections (default=%d)",mDBportno);
    exit(1);
}

/* ------------------------------------------------------------------------- */
/* Open a server socket, return a file descriptor ready for accept() or
 * select() or -1 on error. port=????, addr=INADDR_ANY
 */
int server_socket(long addr,short port) {
    int s;
    struct sockaddr_in sa;

    if ((s=socket(AF_INET,SOCK_STREAM,0))==-1) return -1;
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(addr);
    sa.sin_port        = htons(port);
    if (bind(s,(struct sockaddr *)&sa,sizeof(sa))==-1) return -1;
    if (listen(s,100)==-1) return -1;		/* max 100 in queue */

    return s;
}

void cleanup_conn(int i, int fd) {
    close (fd);
    pollfds[i].fd=-1;
    free(session[i]);
}

void sig_handler(int sig) {
    FILE *fp;
    int  i;
    mDB_exit();
    perror("Received signal, shutting down.");
    for (i=1; i<=contot; i++) if (pollfds[i].fd>0) cleanup_conn(i,pollfds[i].fd);
    exit(1);
}

void net_showoutput(mDBF *mDB, char *s) {
    if (netsocket) {
        if (mDB) {
            char	buf[551];
            sprintf(buf,"%lu|%s",mDB->recno,s);
            net_send(netsocket,buf);
        } else
            net_send(netsocket,s);
    }
}

/* ---------------------------------------------------------------- */
int parse_dbcommand(ses,data,stat)
struct tcp_ses *ses;
char   *data, *stat;
{
    char param[513],fn[81],cmd[2],blank[11],key[81];
    long idxno,norows;
    int  st;

    netsocket=ses->socket_fd;
    /* string contains cmd, fn, idx, norows, key, data,  seperateid by \t */

    getfield  (1,data,cmd  ,  1,9);
    getfield  (2,data,param, 40,9);
    if (strchr(param,'.')!=0) {
        strcpy(stat,"909 ERR"); 		/* '.' not allowed in filename */
        return(0);
    }
    strcpy(fn,param);			/* filename */

    getfldlong(3,data,&idxno   ,9);
    getfldlong(4,data,&norows  ,9);
    getfield  (5,data,key  , 80,9);
    getfield  (6,data,blank, 10,9);
    getfield  (7,data,param,512,9);
    /*  ---------- for debugging connections : */
    if (LOGGING) {
        time_t    t;
        struct tm *d;
        t=time(0);
        d=localtime(&t);
        printf("%04d%02d%02d %02d.%02d.%02d:%s:",
               1900+d->tm_year,d->tm_mon+1,d->tm_mday,
               d->tm_hour,d->tm_min,d->tm_sec,
               ses->ipadr);
        printf("cmd=[%s],fn=[%s],[%lu],[%lu],key=[%s],[%s]%s\n",
               cmd,fn,idxno,norows,key,blank,param);
    }

    if (strchr("ck",cmd[0])) {
        switch (cmd[0]) {
        case 'c' :
            if (mDB_create(fn,param)==0) st=902;
            break;
        case 'k' :
            if (mDB_kill(fn)==0) st=903;
            break;
        }
    } else {
        switch (cmd[0]) {
        case 'a' :
            st=mDB_add(fn,param,0);
            break;
        case 'b' :
            st=mDB_createindex(fn,idxno,key);
            break;
        case 'd' :
            st=mDB_delete(fn,idxno,key);
            break;
        case 'e' :
            st=mDB_find(fn,idxno,key,0);
            break;
        case 'f' :
            st=mDB_first(fn,idxno,norows,0);
            break;
        case 'i' :
            st=mDB_header(fn,0,0);
            break;
        case 'I' :
            st=mDB_fields(fn,0);
            break;
        case 'l' :
            st=mDB_last (fn,idxno,norows,0);
            break;
        case 'n' :
            st=mDB_next(fn,idxno,key,norows,0);
            break;
        case 'p' :
            st=mDB_prev(fn,idxno,key,norows,0);
            break;
        case 'r' :
            st=mDB_read(fn,idxno,0);
            break;
        case 's' :
            st=mDB_search(fn,idxno,key,norows,0);
            break;
        case 'u' :
            st=mDB_update(fn,idxno,key,blank,param,0);
            break;
        case 'x' :
            st=mDB_dropindex(fn,idxno);
            break;
        case 'O' :
            st=mDB_open(fn);
            break;
        case 'C' :
            st=mDB_close(fn);
            break;
        case 'L' :
            st=mDB_log(fn,key);
            break;
        default  :
            st=999;
            break;
        }
    }
    if (st==1)
        strcpy(stat,"100 OK");
    else
        sprintf(stat,"%03d ERR",st?st:200);
    return(1);  /* continue connection */
}

/* ---------------------------- MAIN LOOP ---------------------------------- */
int do_connections() {
    struct  sockaddr_in caddr;
    int 	  ssockfd, csockfd;
    int 	  clen,i,n,nready;
    char 	  buf[513],stat[12];
    time_t  ct,t;

    signal(SIGPIPE,SIG_IGN);
    signal(SIGINT ,sig_handler);
    signal(SIGHUP ,sig_handler);
    signal(SIGQUIT,sig_handler);
    signal(SIGTERM,sig_handler);

    pollfds[0].fd     = ssockfd = server_socket(INADDR_ANY,mDBportno);
    pollfds[0].events = POLLRDNORM;

    /* Initialize pollfds array with server, client info */
    clen=sizeof(caddr);
    contot=0;			    /* Just the server initially, no clients */
    for (i=1; i<MAXCONN; i++)  pollfds[i].fd=-1;

    while (1) {
        nready=poll(pollfds,contot+1,WAKEUPTM);		/* wakeup each 5 secs */
        /* Server, need to add new client */
        if (pollfds[0].revents & POLLRDNORM) {
            csockfd=accept(ssockfd,(struct sockaddr *) &caddr, &clen);
            for (i=1; i<MAXCONN; i++)
                if (pollfds[i].fd<0) {
                    pollfds[i].fd=csockfd;
                    session[i]=malloc(sizeof(struct tcp_ses));
                    session[i]->timeout=time(0)+MAXIDLE;
                    session[i]->socket_fd=csockfd;
                    strcpy(session[i]->ipadr,(char *)inet_ntoa(caddr.sin_addr));
                    break;
                }
            if (i==MAXCONN)
                perror ("max clients reached");
            else {
                pollfds[i].events=POLLRDNORM;
                if (i>contot)  contot=i;
                if (--nready<=0) continue;
            }
        } /*if*/

        ct=time(0);
        /* ------- Now service clients in ascending order -------- */
        for (i=1; i<=contot; i++) {
            if ((csockfd=pollfds[i].fd)<0)   continue;
            if (pollfds[i].revents & (POLLRDNORM | POLLERR)) {
                if ((n=net_receive(csockfd,buf,512))>=0) {
                    if (n == 0) {
                        cleanup_conn(i,csockfd);
                    } else {
                        n=parse_dbcommand(session[i],buf,stat);
                        if (n==0 || net_send(csockfd,stat)<=0) {
                            cleanup_conn(i,csockfd);
                        } else
                            session[i]->timeout=time(0)+MAXIDLE;
                    }
                    if (--nready <= 0) break;
                }
            } else if (ct>session[i]->timeout) {
                cleanup_conn(i,csockfd);
            }
        } /*for*/
        if (contot) {
            while (contot>0 && pollfds[contot].fd==-1) contot--;
            if (!contot)  {
                mDB_flushall();
            }
        }
    } /*while*/
}

int main(int argc, char *argv[]) {
    FILE  *fp;
    char  ch;
    char  mDBDIR[31]="";		/* default database directory */
    pid_t pid;

    while ((ch=getopt(argc,argv,"d:p:l"))!=-1) {
        switch (ch) {
        case 'd' :
            strcpy(mDBDIR,optarg);
            break;
        case 'p' :
            mDBportno=atoi(optarg);
            break;
        case 'l' :
            LOGGING=1;
            break;
        default  :
            syntax();
            break;
        }
    }
    argc -=optind;
    argv +=optind;
    if (mDBDIR[0]!=0 && chdir(mDBDIR)) {
        perror("Unable to switch to database directory");
        exit(1);
    }

    if (fork()!=0)  exit(0);
    pid=setsid();

    umask(0);
    mDB_init();
    mDB_output=net_showoutput;	/* redirect the isam output to the
				   network clients */
    do_connections();			/* handle the client connections */
    mDB_exit();
    exit(0);
}

