/* -------------------------------------------------------------------------- *
 * Description : tcp socket functions and integrating some basic
 *               communication prototcol between the minidb client/server.
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
#include <string.h>
#include <netdb.h>
#include "mdb_net.h"

/* net_lookup:
 * Trivial DNS lookup routine that accepts a string that can contain a
 * host name or IP address
 * returns: 0 on error or the host address in network order.
 */
unsigned long net_lookup(char *host) {
    struct hostent *he;
    if ((he=gethostbyname(host)) == NULL) {
        if ((he=gethostbyaddr(host, strlen(host), AF_INET)) == NULL) return 0;
    }
    return *(long*)(he->h_addr_list[0]);
}

/* net_open:
 * opens a network tcp connection,
 * host can be a hostname or ip address
 * returns: 0 on error, or the socket fd on success
 */
int net_open(char *host, int port) {
    unsigned long adr;
    struct   sockaddr_in svradr;
    int      fd,st;
    /* Create a socket for the client. */
    adr=net_lookup(host);
    if (adr==0) return(0);
    fd=socket(AF_INET, SOCK_STREAM, 0);
    /* Name the socket, as agreed with the server. */
    svradr.sin_family      = AF_INET;
    svradr.sin_addr.s_addr = adr;
    svradr.sin_port        = htons(port);
    /* Now connect our socket to the server's socket. */
    st=connect(fd, (struct sockaddr *)&svradr, sizeof(svradr) );
    if (st == -1)  return(0);
    return(fd);
}
/* net_receive:
 * receive message from client or server, layout:
 * 1st 2 bytes = total length of real msg
 *     n bytes = message
 * returns: 0 on error, or length of received data
 */
int net_receive(int fd, char *data, int max) {
    unsigned char msb,lsb;
    int 		n,len;

    data[0]=0;				/* clear data */
    if (read(fd,&msb,1)<=0) return(0);
    if (read(fd,&lsb,1)<=0) return(0);
    len=(msb*256)+lsb;			/* calc size of data */
    if (len>=max) return(0);		/* check if it fits  */
    n=read(fd,data,len);
    if (n!=len)   return(0);		/* check correctly received */
    data[len]=0;
    return(len);
}

/* net_send:
 * send a message from client or server, layout:
 * 1st 2 bytes = total length of message
 *     n bytes = message
 * returns: 0 on error, or length of send data
 */
int net_send(int fd, char *data) {
    unsigned char msb,lsb;
    int 	 	len;

    len=strlen(data);
    if (len<=0 || len>1024)       return(0);    /* enforce size limitations */
    msb=len/256;
    lsb=len%256;
    if (write(fd,&msb,1)!=1)  	return(0);
    if (write(fd,&lsb,1)!=1)  	return(0);
    if (write(fd,data,len)!=len)  return(0);
    return(len);
}

/* net_sndsend:
 * combined function to report data and endstatus, same as
 * doing 2x net_send
 * returns: 0 on error, or 1 on success
 */
int net_sndresult(int fd, char *data, char *stat) {
    if (data && net_send(fd,data)<=0) return(0);
    if (stat && net_send(fd,stat)<=0) return(0);
    return(1);
}

/* net_close:
 * signal other side we are quitting and close the network socket./
 */
void net_close(int fd) {
    net_send(fd,"QUIT");		/* signal other side we are quitting */
    close(fd);
}

