/* -------------------------------------------------------------------------- *
 * Description : ISAM-BTREE library with a small footprint
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
#ifndef ISAM_LIB_H
#define ISAM_LIB_H

#include <time.h>
#include "bt.h"

#define UL	 	unsigned long
#define MDB_KEYMAX	10

/* ----------------------- mDB_header() types ------------------------------ */
#define MDB_DATES 	1
#define MDB_RECORD 	2
#define MDB_CREATE 	3
#define MDB_FIELDS 	4


typedef struct {
  char     	fn[31];		/* filename */
  /* ----------------------------- DATA HEADER INFO ------------------------ */
  char 		ID[8];		/* FILEID = ISBT<version> (ex: ISBT1.0) */
  int  		recsize;	/* record size, 1e ch is recordstate ' '/'D' */
  int  		datastart;	/* start position of datarecords */
  time_t 	creation;	/* creation time    (secs1970) */
  time_t 	updtime;	/* last update time (secs1970) */
  UL 		delno;		/* no deleted */
  UL 		dellist;	/* first deleted record */
  UL		autoincr;	/* autoincrement counter */
  int		logging;	/* logging of insert/update/delete actions */
  int  		fldno;		/* no of fields */
  int  		fldbytes;	/* size of all fieldnames */
  int  		keyno;		/* no of keys */
  int  		key[MDB_KEYMAX][8]; /* max room for 10 key of max 8 fields */
  /* ----------------------------- RUNTIME VARS ---------------------------- */
  int      	no;		/* no times opened */
  FILE     	*fp;		/* filepointer of datafile */
  FILE     	*logfp;		/* filepointer of loggingfile */
  UL		records;	/* number of records */
  UL		recno;		/* current recordnumber  */
  /* ----------------------------- ALLOCATED MEMORY ------------------------ */
  btFP  	*idx[MDB_KEYMAX];	/* index filepointers */
  char 		*fldnms;	/* ptr to allocated room for all fieldnames */
  char    	**fld;		/* runtime array of pointers to fldnames */
  char    	**fldtype;
  int     	*fldofs;
  char    	*record;	/* fixed datarecord */
  char    	*varrec;	/* variable length record with seperator */
  /* ----------------------------- PREVIOUS/NEXT POINTERS ------------------ */
  void	   	*prev;		/* prev dbfile info */
  void     	*next;		/* next dbfile info */
} mDBF;

typedef void (mDBOUTPUT) (mDBF *, char *);

extern mDBF 	*mDBFS;			/* Linklist off open database files */
extern mDBOUTPUT *mDB_output;		/* output function */

extern char	mDBhostname[41];	/* in case of client/server */
extern int	mDBportno;

/* -------------------------------------- INTERNAL USED function */
void 	mDBi_showoutput(mDBF *,char *);
int	mDBi_writehdr(mDBF *);
int	mDBi_readhdr(mDBF *,int);
mDBF *	mDBi_fn(char *);
mDBF *	mDBi_addfn(char *);
void 	mDBi_delfn(mDBF *);
mDBF *	mDBi_open(char *,int);
int	mDBi_close(mDBF *);
void	mDBi_fieldfmt(mDBF *,char *,char *,char *);
void	mDBi_rec2field(mDBF *,int);
void	mDBi_rec2fields(mDBF *);
void	mDBi_field2rec(mDBF *,int,char *,char *);
void	mDBi_buildkey(mDBF *,int,char *);
void	mDBi_updkey(mDBF *,char,UL);
int	mDBi_idxopen(mDBF *,int);
void	mDBi_idxclose(mDBF *,int);
void	mDBi_writedata(mDBF *,int,char *,char *);
void	mDBi_log(mDBF *,char);
int 	mDBi_addrec(mDBF *,char *,char *);
int 	mDBi_read(mDBF *,UL,int,char *);
void	mDBi_inp2key(mDBF *,int,char *,char *);
int	mDBi_equal(mDBF *,int,char *,char *,UL *);


/* ------------------------------------------ USER functions */
int	mDB_fields(char *,char *);
int	mDB_header(char *,int,char *);
int	mDB_create(char *,char *);
int	mDB_open(char *);
int	mDB_close(char *);

int 	mDB_add(char *,char *,char *);
int	mDB_update(char *,int,char *,char *,char *,char *);
int	mDB_delete(char *,int,char *);

int 	mDB_find(char *,int,char *,char *);
int 	mDB_search(char *,int,char *,int,char *);
int 	mDB_read(char *,UL,char *);

int 	mDB_first(char *,int,int,char *);
int 	mDB_last(char *,int,int,char *);

int 	mDB_prev(char *,int,char *,int,char *);
int 	mDB_next(char *,int,char *,int,char *);

int 	mDB_dropindex(char *,int);
int 	mDB_createindex(char *,int,char *);
int     mDB_rebuildindex(char *,int);
int     mDB_log(char *,char *);

int	mDB_kill();
int	mDB_option(char *,char *,char *);
int	mDB_init();
void	mDB_exit();

#endif
