/* -------------------------------------------------------------------------- *
 * Description : A 'simple' and compact btree implementation.
 *
 * Code is based on the sourcecode of B++-trees (s_btr.c) which is described
 * in:  A Compact Guide to Sorting and Searching by Thomas Niemann
 *
 * The website says:
 *
 * Permission to reproduce portions of this document is given provided the 
 * web site listed below is referenced, and no additional restrictions apply. 
 * Source code, when part of a software project, may be used freely without 
 * reference to the author.
 *
 * Thomas Niemann, Portland, Oregon  epaperpress.com 
 * Website: http://www.oopweb.com/Algorithms/Documents/Sman/VolumeFrames.html
 *
 *
 * Modifications:
 * - added code for duplicate keys, some open ends in the original code.
 * - rewrite of search function
 * - flushing of data to disk.
 * - user functions redefined
 * - removed all global data
 * - removed counters
 * - changed btFindKey function
 * - added btSearchKey function
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
#ifndef BT_H
#define BT_H

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

/*
 * this file is divided into sections:
 *   stuff you'll probably want to place in a .h file...
 *     implementation dependent
 *       - you'll probably have to change something here
 *     implementation independent
 *       - types and function prototypes that typically go in a .h file
 *     function prototypes
 *       - prototypes for user functions
 *   internals
 *     - local functions
 *     - user functions
 *   main()
 */

#define SECTORSIZE	1024
typedef enum { MODE_FIRST, MODE_MATCH } modeEnum;
/****************************
 * implementation dependent *
 ****************************/
typedef unsigned long eAdrType;       /* record address for external record */
typedef unsigned long bAdrType;       /* record address for btree node */

#define CC_EQ           0
#define CC_GT           1
#define CC_LT          -1

/* compare two keys and return:
 *    CC_LT     key1 < key2
 *    CC_GT     key1 > key2
 *    CC_EQ     key1 = key2
 */
typedef int (*btCmpFunc)(const void *key1, const void *key2,int size);

/******************************
 * implementation independent *
 ******************************/

typedef enum {false, true} bool;
#define bOk		0
#define bKeyFound	0
#define bHigherKeyFound	1

#define bErrKeyNotFound	-1
#define bErrDupKeys	-2
#define bErrEndOfIndex	-3
#define bErrSectorSize	-4
#define bErrFileNotOpen	-5
#define bErrIO		-6
#define bErrMemory	-7


/*************
 * internals *
 *************/

/*
 *  algorithm:
 *    A B+tree implementation, with keys stored in internal nodes,
 *    and keys/record addresses stored in leaf nodes.  Each node is
 *    one sector in length, except the root node whose length is
 *    3 sectors.  When traversing the tree to insert a key, full
 *    children are adjusted to make room for possible new entries.
 *    Similarly, on deletion, half-full nodes are adjusted to allow for
 *    possible deleted entries.  Adjustments are first done by 
 *    examining 2 nearest neighbors at the same level, and redistibuting
 *    the keys if possible.  If redistribution won't solve the problem,
 *    nodes are split/joined as needed.  Typically, a node is 3/4 full.
 *    On insertion, if 3 nodes are full, they are split into 4 nodes,
 *    each 3/4 full.  On deletion, if 3 nodes are 1/2 full, they are
 *    joined to create 2 nodes 3/4 full.
 *
 *    A LRR (least-recently-read) buffering scheme for nodes is used to
 *    simplify storage management, and, assuming some locality of reference,
 *    improve performance.
 *
 *    To simplify matters, both internal nodes and leafs contain the
 *    same fields.
 *   
 */

/* macros for addressing fields */

/* primitives */
#define bAdr(p) *(bAdrType *)(p)
#define eAdr(p) *(eAdrType *)(p)

/* based on k = &[key,rec,childGE] */
#define childLT(k) bAdr((char *)k - sizeof(bAdrType))
#define key(k) (k)
#define rec(k) eAdr((char *)(k) + h->keySize)
#define childGE(k) bAdr((char *)(k) + h->keySize + sizeof(eAdrType))

/* based on b = &btBuf */
#define leaf(b) b->p->leaf
#define ct(b) b->p->ct
#define next(b) b->p->next
#define prev(b) b->p->prev
#define fkey(b) &b->p->fkey
#define lkey(b) (fkey(b) + ks((ct(b) - 1)))
#define p(b) (char *)(b->p)

/* shortcuts */
#define ks(ct) ((ct) * h->ks)

typedef char btKey;             /* keys entries are treated as char arrays */

typedef struct {
    unsigned int leaf:1;        /* first bit = 1 if leaf */
    unsigned int ct:15;         /* count of keys present */
    bAdrType prev;              /* prev node in sequence (leaf) */
    bAdrType next;              /* next node in sequence (leaf) */
    bAdrType childLT;           /* child LT first key */
    /* ct occurrences of [key,rec,childGE] */
    btKey    fkey;               /* first occurrence */
} btNode;

typedef struct bufTypeTag {     /* location of node */
    struct bufTypeTag *next;    /* next */
    struct bufTypeTag *prev;    /* previous */
    bAdrType 	adr;            /* on disk */
    btNode   	*p;             /* in memory */
    bool 	valid;          /* true if buffer contents valid */
    bool 	modified;       /* true if buffer modified */
} btBuf;

/* one node for each open handle */
typedef struct {
    FILE   *fp;                /* idx file */
    int    keySize;            /* key length */
    int    sectorSize;         /* block size for idx records */
    bool   dupKeys;	       /* unique or duplicates */
    btBuf  root;               /* root of b-tree, room for 3 sets */
    btBuf  bufList;            /* head of buf list */
    void   *malloc1;           /* malloc'd resources */
    void   *malloc2;           /* malloc'd resources */
    btBuf  gbuf;               /* gather buffer, room for 3 sets */
    btBuf  *curBuf;            /* current location */
    btKey  *curKey;            /* current key in current node */
    unsigned int maxCt;        /* minimum # keys in node */
    int    ks;                 /* sizeof key entry */
    bAdrType nextFreeAdr;      /* next free b-tree record address */
} btFP;



/***********************
 * function prototypes *
 ***********************/
btFP *btOpen(char *name, int keySize, int dupKeys);
    /*
     * input:
     *   name                   info for open
     *   keysize                size of key
     *   dupkeys                duplicates allowed
     * output:
     *   handle                 handle to btree, used in subsequent calls
     * returns:
     *   bErrOk                 open was successful
     *   bErrMemory             insufficient memory
     *   bErrSectorSize         sector size too small or not 0 mod 4
     *   bErrFileNotOpen        unable to open index file
     */

int btClose(btFP *h);
    /*
     * input:
     *   handle                 handle returned by bOpen
     * returns:
     *   bErrOk                 file closed, resources deleted
     */

int btInsertKey(btFP *h, void *key, eAdrType rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     *   key                    key to insert
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrDupKeys            duplicate keys
     */

int btDeleteKey(btFP *h, void *key, eAdrType rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     *   key                    key to delete
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */

int bFindKey_i(btFP *h, void *key, eAdrType krec, bool Mode, eAdrType *rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     *   key                    key to find
     * output:
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */
#define btSearchKey(h,key,rec)	  bFindKey_i(h,key,0,MODE_FIRST,rec)
#define btFindKey(h,key,rec)      bFindKey_i(h,key,*rec,MODE_MATCH,rec)

int btFindFirstKey(btFP *h, void *key, eAdrType *rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     * output:
     *   key                    first key in sequential set
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */

int btFindLastKey(btFP *h, void *key, eAdrType *rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     * output:
     *   key                    last key in sequential set
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */

int btFindNextKey(btFP *h, void *key, eAdrType *rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     * output:
     *   key                    key found
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */

int btFindPrevKey(btFP *h, void *key, eAdrType *rec);
    /*
     * input:
     *   handle                 handle returned by bOpen
     * output:
     *   key                    key found
     *   rec                    record address
     * returns:
     *   bErrOk                 operation successful
     *   bErrKeyNotFound        key not found
     */

#endif
