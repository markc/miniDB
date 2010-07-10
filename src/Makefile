# ------------------------------------------------------------------------- #
# $Id: Makefile,v 2.0 2003/10/06 20:36:44 harder Exp $
#
# $RCSfile: Makefile,v $
# $Date: 2003/10/06 20:36:44 $
# $Revision: 2.0 $
#
# $Log: Makefile,v $
# Revision 2.0  2003/10/06 20:36:44  harder
# New btree library and mdb2xml, mdbfilter programs
#
# Revision 1.0  2003/03/23 20:17:31  harder
# Initial revision
#
# ------------------------------------------------------------------------- #
CC=gcc
DEFS=  
INCLUDES= -I.
LIBS=

PROGNAME=mdb mdbclient mdbserver mdbfilter mdb2xml mdb2json

# replace -O with -g in order to debug
DEFINES= $(INCLUDES) $(DEFS) -DSYS_UNIX=1
CFLAGS= -O $(DEFINES)

all:       $(PROGNAME)

mdb:       mdb.o mdb_lib.o bt.o mdb_func.o
	   $(CC) $(CFLAGS) -o mdb mdb.o mdb_lib.o bt.o mdb_func.o $(LIBS)
	   strip mdb

mdbclient: mdb.o mdb_client.o mdb_net.o mdb_func.o
	   $(CC) $(CFLAGS) -o mdbclient mdb.o mdb_client.o \
		              mdb_net.o mdb_func.o $(LIBS)
	   strip mdbclient

mdbserver: mdbserver.o mdb_lib.o mdb_net.o mdb_func.o bt.o
	   $(CC) ${CFLAGS} -o mdbserver mdbserver.o mdb_lib.o \
		   	      mdb_net.o mdb_func.o bt.o ${LIBS}
	   strip mdbserver

mdbfilter: mdbfilter.o mdb_func.o
	   $(CC) $(CFLAGS) -o mdbfilter mdbfilter.o mdb_func.o $(LIBS)
	   strip mdbfilter

mdb2xml:   mdb2xml.o mdb_func.o
	   $(CC) $(CFLAGS) -o mdb2xml mdb2xml.o mdb_func.o $(LIBS)
	   strip mdb2xml

mdb2json:  mdb2json.o mdb_func.o
	   $(CC) $(CFLAGS) -o mdb2json mdb2json.o mdb_func.o $(LIBS)
	   strip mdb2json

mdb_client.o: mdb_client.c
mdb_lib.o   : mdb_lib.c mdb_lib.h
mdb_func.o  : mdb_func.c mdb_func.h
mdb_net.o   : mdb_net.c mdb_net.h
bt.o        : bt.c bt.h

clean:
	   rm -f *.o $(PROGNAME) core

