/* -------------------------------------------------------------------------- *
 * Description : ISAM library wrapper functions
 *
 * ------------------------------------------------------------------------- *
 *
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
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <signal.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#define UL	unsigned long

typedef struct {
    int	status;
    UL 	recno;
    char  *varrec;
} mDBF;

typedef void (mDBOUTPUT) (mDBF *, char *);

void 	 mDBi_showoutput(mDBF *, char *);
mDBOUTPUT *mDB_output=mDBi_showoutput;

char EMPTYID[31]="#";
char mDBhostname[41] = "127.0.0.1";
int  mDBportno       = 7221;
int  mDBsockfd       = 0;

/* ------------------------------------------------------------------ */
int snd_data(char *data) {
    int st;
    if (mDBsockfd==0 || (st=net_send(mDBsockfd,data))<=0) {
        mDBsockfd=net_open(mDBhostname,mDBportno);		/* try reconnecting */
        if (mDBsockfd) st=net_send(mDBsockfd,data);
    }
    return(st);
}

int chk_resultcode(char *data, char *stat) {
    int i;
    char *st[] = {"OK", "ERR", 0 };

    *stat=0;
    if (isdigit(data[0]) && isdigit(data[1])
            && isdigit(data[2]) && data[3]==' ') {
        for (i=0; (st[i] && strcmp(&data[4],st[i])!=0); i++);
        if (st[i]) {
            strcpy(stat,&data[4]);
            return(atoi(data));
        }
    }
    return(0);
}
/*
 * returns: 0=error, 1=data received, >=100 statusline
 */
int recv_data(char *data, int max, char *status) {
    int n,r;

    data[0]=status[0]=0;
    n=net_receive(mDBsockfd,data,max);
    if (n==0) return(0);              /* error, break connection */

    n=chk_resultcode(data,status);
    if (n)    return(n);		/* no more data, return status */

    return(1);		        /* received data, more to come */
}

/* ------------------------------------------------------------------------ */

void mDBi_showoutput(mDBF *mDB, char *s) {
    printf("%s\n",s);
}

int mDBi_msg_s(msg,src)
char *msg,*src;
{
    strcat(msg,"\t");
    strcat(msg,src?src:" ");
}

int mDBi_msg_i(msg,n)
char *msg;
UL   n;
{
    char str[16];

    sprintf(str,"\t%ld",n);
    strcat(msg,str);
}

int mDBi_msg(cmd,fn,idx,rows,key,blank,data,dst)
char cmd,*fn,*key,*blank,*data,*dst;
long idx,rows;
{
    mDBF  mDB;
    char mDBmsg[550], stat[21];			/* communication buffer */
    int  st;
    sprintf(mDBmsg,"%c\t%s",cmd,fn);

    mDBi_msg_i(mDBmsg,idx);
    mDBi_msg_i(mDBmsg,rows);
    mDBi_msg_s(mDBmsg,key);
    mDBi_msg_s(mDBmsg,blank);
    mDBi_msg_s(mDBmsg,data);

    mDB.varrec=mDBmsg;
    snd_data(mDBmsg);
    while ((mDB.status=recv_data(mDBmsg,512,stat))==1) {
        if (dst) strcpy(dst,mDBmsg);
        else if (mDB_output) mDB_output(&mDB,mDBmsg);
    }
    if (mDB.status==100)  return(1);
    return mDB.status;
}

int mDB_init() {
    signal(SIGPIPE,SIG_IGN);
    /*  We can now read/write via sockfd.  */
    mDBsockfd=net_open(mDBhostname,mDBportno);
    if (mDBsockfd <= 0) {
        puts("Unable to connect");
        exit(1);
    }

}

void mDB_exit() {
    net_close(mDBsockfd);
}

int mDB_fields(fn,dst) {	/* get fieldnames info */
    return mDBi_msg('I',fn,0,0,0,0,0,dst);
}

int mDB_header(fn,type,dst) {	/* get file-isam info */
    return mDBi_msg('i',fn,type,0,0,0,0,dst);
}

int mDB_create(fn,flds) {	/* create a datafile */
    return mDBi_msg('c',fn,0,0,0,0,flds,0);
}

int mDB_add(fn,data,dst) {
    return mDBi_msg('a',fn,0,0,0,0,data,dst);
}

int mDB_update(fn,idx,key,blank,data,dst) {
    return mDBi_msg('u',fn,idx,0,key,blank,data,dst) ;
}

int mDB_delete(fn,idx,key) {
    return mDBi_msg('d',fn,idx,0,key,0,0,0);
}

int mDB_find(fn,idx,key,dst) {
    return mDBi_msg('e',fn,idx,0,key,0,0,dst);
}

int mDB_read(fn,recno,dst) {
    return mDBi_msg('r',fn,recno,0,0,0,0,dst);
}

int mDB_search(fn,idx,key,rows,dst) {
    return mDBi_msg('s',fn,idx,rows,key,0,0,dst);
}

int mDB_first(fn,idx,rows,dst) {
    return mDBi_msg('f',fn,idx,rows,0,0,0,dst);
}

int mDB_last(fn,idx,rows,dst) {
    return mDBi_msg('l',fn,idx,rows,0,0,0,dst);
}

int mDB_next(fn,idx,key,rows,dst) {
    return mDBi_msg('n',fn,idx,rows,key,0,0,dst);
}

int mDB_prev(fn,idx,key,rows,dst) {
    return mDBi_msg('p',fn,idx,rows,key,0,0,dst);
}

int mDB_open(fn) {
    return mDBi_msg('O',fn,0,0,0,0,0,0);
}

int mDB_createindex(fn,idx,keydef) {
    return mDBi_msg('A',fn,idx,0,keydef,0,0,0);
}

int mDB_dropindex(fn,idx) {
    return mDBi_msg('D',fn,idx,0,0,0,0,0);
}

int mDB_rebuildindex(fn,idx) {
    return mDBi_msg('B',fn,idx,0,0,0,0,0);
}

int mDB_log(fn,onoff) {
    return mDBi_msg('L',fn,0,0,onoff,0,0,0);
}

int mDB_kill(fn) {
    return mDBi_msg('k',fn,0,0,0,0,0,0);
}

int mDB_close(fn) {
    return mDBi_msg('C',fn,0,0,0,0,0,0);
}

