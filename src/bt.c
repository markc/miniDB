/* -------------------------------------------------------------------------- *
 * $RCSfile: bt.c,v $
 * $Date: 2003/10/06 20:33:37 $
 * $Revision: 1.0 $
 *
 * Description : A 'simple' and compact btree implementation.
 * Author      : Hans Harder
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
 * Modifications done:
 * - added code for duplicate keys, some open ends in the original code.
 * - rewrite of search function.
 * - flushing of data to disk.
 * - user functions redefined
 * - removed all global data
 * - removed counters
 * - changed btFindKey function
 * - added btSeachKey function
 * ------------------------------------------------------------------------- *
 * $Log: bt.c,v $
 * Revision 1.0  2003/10/06 20:33:37  harder
 * Initial revision
 *
 * ------------------------------------------------------------------------- *
 * Copyright © 2003,2002 Hans Harder.
 *
 * All Rights Reserved.
 *
 * PERMISSION IS GRANTED TO DISTRIBUTE THIS SOFTWARE FREELY.
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for educational, research, personal, corporate and
 * non-profit purposes, without fee, and without a written agreement is hereby
 * granted for all cases, provided that the above copyright notice, this
 * paragraph, and the following three paragraphs appear in all copies.
 *
 * Permission to incorporate this software, without fee, into commercial
 * products is hereby granted.
 *
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT,
 * SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OF THIS
 * SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE
 * AUTHOR HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 *
 * ------------------------------------------------------------------------- */
static char const RCSid_bt[]="$Id: bt.c,v 1.0 2003/10/06 20:33:37 harder Exp $";

#include "bt.h"


static bAdrType allocAdr(btFP *h) {
    bAdrType adr;
    adr = h->nextFreeAdr;
    h->nextFreeAdr += h->sectorSize;
    return adr;
}

static int flush(btFP *h, btBuf *buf) {
    int len;            /* number of bytes to write */

    /* flush buffer to disk */
    len = h->sectorSize;
    if (buf->adr == 0) len *= 3;        /* root */
    if (fseek(h->fp, buf->adr, SEEK_SET)) return bErrIO;
    if (fwrite(buf->p, len, 1, h->fp) != 1) return bErrIO;
    buf->modified = false;
    return bOk;
}

static int flushAll(btFP *h) {
    int rc;                /* return code */
    btBuf *buf;              /* buffer */

    if (h->root.modified)
        if ((rc = flush(h, &h->root)) != 0) return rc;

    buf = h->bufList.next;
    while (buf != &h->bufList) {
        if (buf->modified)
            if ((rc = flush(h, buf)) != 0) return rc;
        buf = buf->next;
    }
    /* flush it also to disk ! */
    fflush(h->fp);
    return bOk;
}

static int assignBuf(btFP *h,bAdrType adr, btBuf **b) {
    /* assign buf to adr */
    btBuf *buf;              /* buffer */
    int rc;                /* return code */

    if (adr == 0) {
        *b = &h->root;
        return bOk;
    }

    /* search for buf with matching adr */
    buf = h->bufList.next;
    while (buf->next != &h->bufList) {
        if (buf->valid && buf->adr == adr) break;
        buf = buf->next;
    }

    /* either buf points to a match, or it's last one in list (LRR) */
    if (buf->valid) {
        if (buf->adr != adr) {
            if (buf->modified) {
                if ((rc = flush(h,buf)) != 0) return rc;
            }
            buf->adr = adr;
            buf->valid = false;
        }
    } else {
        buf->adr = adr;
    }

    /* remove from current position and place at front of list */
    buf->next->prev = buf->prev;
    buf->prev->next = buf->next;
    buf->next = h->bufList.next;
    buf->prev = &h->bufList;
    buf->next->prev = buf;
    buf->prev->next = buf;
    *b = buf;
    return bOk;
}

static int writeDisk(btBuf *buf) {
    /* write buf to disk */
    buf->valid = true;
    buf->modified = true;
    return bOk;
}

static int readDisk(btFP *h, bAdrType adr, btBuf **b) {
    /* read data into buf */
    int len;
    btBuf *buf;               /* buffer */
    int rc;                /* return code */

    if ((rc = assignBuf(h, adr, &buf)) != 0) return rc;
    if (!buf->valid) {
        len = h->sectorSize;
        if (adr == 0) len *= 3;         /* root */
        if (fseek(h->fp, adr, SEEK_SET)) return bErrIO;
        if (fread(buf->p, len, 1, h->fp) != 1) return bErrIO;
        buf->modified = false;
        buf->valid = true;
    }
    *b = buf;
    return bOk;
}


static int search(
    btFP  *h,
    btBuf *buf,
    void  *key,
    eAdrType recno,
    btKey **mkey,
    modeEnum mode,
    int sw) {
    /*
     * input:
     *   p                      pointer to node
     *   key                    key to find
     *   recno                  record address
     *   mode                   MODE_MATCH = recno must be the same
     * output:
     *   k                      pointer to btKey info
     * returns:
     *   CC_EQ                  key = mkey
     *   CC_LT                  key < mkey
     *   CC_GT                  key > mkey
     */
    int cc;                     /* condition code */
    int lb;                     /* lower-bound of binary search */
    int ub;                     /* upper-bound of binary search */
    btBuf *mbuf;              /* buffer */

    mbuf=buf;
    if (ct(mbuf) == 0) {     /* empty list */
        *mkey = fkey(mbuf);
        return CC_LT;
    }
    cc=-1;
    /* scan current node for key without a binary search */
    /* if no match fetch always the next key, even if outside buf */
    do {
        ub = ct(mbuf);
        for (lb=0; lb<ub; lb++) {
            *mkey = fkey(mbuf) + ks(lb);
            cc = memcmp(key, key(*mkey), h->keySize);
            /* ------ recordno compare if needed ------ */
            if (cc==0 && mode==MODE_MATCH && recno) {
                if (recno == rec(*mkey)) break;
                if (recno <  rec(*mkey)) continue;
                cc=-1;
            }
            if (cc<=0) break;
        }
        h->curBuf = mbuf;
        h->curKey = *mkey;
        /* ------ large no of duplicates, fetch next set */
        if (lb==ub) {
            if (sw && next(mbuf)) {          /* is the a next */
                if (readDisk(h, next(mbuf), &mbuf)!=0) return cc;
            } else
                break;   /* break out of the loop */
        }
    } while (lb==ub);

    return cc;
}

static int scatterRoot(btFP *h) {
    btBuf *gbuf;
    btBuf *root;

    /* scatter gbuf to root */

    root = &h->root;
    gbuf = &h->gbuf;
    memcpy(fkey(root), fkey(gbuf), ks(ct(gbuf)));
    childLT(fkey(root)) = childLT(fkey(gbuf));
    ct(root) = ct(gbuf);
    leaf(root) = leaf(gbuf);
    return bOk;
}

static int scatter(btFP *h, btBuf *pbuf, btKey *pkey, int is, btBuf **tmp) {
    btBuf *gbuf;                /* gather buf */
    btKey *gkey;                 /* gather buf key */
    int rc;                   /* return code */
    int iu;                     /* number of tmp's used */
    int k0Min;                  /* min #keys that can be mapped to tmp[0] */
    int knMin;                  /* min #keys that can be mapped to tmp[1..3] */
    int k0Max;                  /* max #keys that can be mapped to tmp[0] */
    int knMax;                  /* max #keys that can be mapped to tmp[1..3] */
    int sw;                     /* shift width */
    int len;                    /* length of remainder of buf */
    int base;                   /* base count distributed to tmps */
    int extra;                  /* extra counts */
    int ct;
    int i;

    /*
     * input:
     *   pbuf                   parent buffer of gathered keys
     *   pkey                   where we insert a key if needed in parent
     *   is                     number of supplied tmps
     *   tmp                    array of tmp's to be used for scattering
     * output:
     *   tmp                    array of tmp's used for scattering
     */

    /* scatter gbuf to tmps, placing 3/4 max in each tmp */

    gbuf = &h->gbuf;
    gkey = fkey(gbuf);
    ct = ct(gbuf);

    /****************************************
     * determine number of tmps to use (iu) *
     ****************************************/
    iu = is;

    /* determine limits */
    if (leaf(gbuf)) {
        /* minus 1 to allow for insertion */
        k0Max= h->maxCt - 1;
        knMax= h->maxCt - 1;
        /* plus 1 to allow for deletion */
        k0Min= (h->maxCt / 2) + 1;
        knMin= (h->maxCt / 2) + 1;
    } else {
        /* can hold an extra gbuf key as it's translated to a LT pointer */
        k0Max = h->maxCt - 1;
        knMax = h->maxCt;
        k0Min = (h->maxCt / 2) + 1;
        knMin = ((h->maxCt+1) / 2) + 1;
    }

    /* calculate iu, number of tmps to use */
    while (1) {
        if (iu == 0 || ct > (k0Max + (iu-1)*knMax)) {
            /* add a buffer */
            if ((rc = assignBuf(h, allocAdr(h), &tmp[iu])) != 0)
                return rc;
            /* update sequential links */
            if (leaf(gbuf)) {
                /* adjust sequential links */
                if (iu == 0) {
                    /* no tmps supplied when splitting root for first time */
                    prev(tmp[0]) = 0;
                    next(tmp[0]) = 0;
                } else {
                    prev(tmp[iu]) = tmp[iu-1]->adr;
                    next(tmp[iu]) = next(tmp[iu-1]);
                    next(tmp[iu-1]) = tmp[iu]->adr;
                }
            }
            iu++;
        } else if (iu > 1 && ct < (k0Min + (iu-1)*knMin)) {
            /* del a buffer */
            iu--;
            /* adjust sequential links */
            if (leaf(gbuf) && tmp[iu-1]->adr) {
                next(tmp[iu-1]) = next(tmp[iu]);
            }
            next(tmp[iu-1]) = next(tmp[iu]);
        } else {
            break;
        }
    }

    /* establish count for each tmp used */
    base = ct / iu;
    extra = ct % iu;
    for (i = 0; i < iu; i++) {
        int n;

        n = base;
        /* distribute extras, one at a time */
        /* don't do to 1st node, as it may be internal and can't hold it */
        if (i && extra) {
            n++;
            extra--;
        }
        ct(tmp[i]) = n;
    }


    /**************************************
     * update sequential links and parent *
     **************************************/
    if (iu != is) {
        /* link last node to next */
        if (leaf(gbuf) && next(tmp[iu-1])) {
            btBuf *buf;
            if ((rc = readDisk(h, next(tmp[iu-1]), &buf)) != 0) return rc;
            prev(buf) = tmp[iu-1]->adr;
            if ((rc = writeDisk(buf)) != 0) return rc;
        }
        /* shift keys in parent */
        sw = ks(iu - is);
        if (sw < 0) {
            len = ks(ct(pbuf)) - (pkey - fkey(pbuf)) + sw;
            memmove(pkey, pkey - sw, len);
        } else {
            len = ks(ct(pbuf)) - (pkey - fkey(pbuf));
            memmove(pkey + sw, pkey, len);
        }
        /* don't count LT buffer for empty parent */
        if (ct(pbuf))
            ct(pbuf) += iu - is;
        else
            ct(pbuf) += iu - is - 1;
    }

    /*******************************
     * distribute keys to children *
     *******************************/
    for (i = 0; i < iu; i++) {

        /* update LT pointer and parent nodes */
        if (leaf(gbuf)) {
            /* update LT, tmp[i] */
            childLT(fkey(tmp[i])) = 0;

            /* update parent */
            if (i == 0) {
                childLT(pkey) = tmp[i]->adr;
            } else {
                memcpy(pkey, gkey, ks(1));
                childGE(pkey) = tmp[i]->adr;
                pkey += ks(1);
            }
        } else {
            if (i == 0) {
                /* update LT, tmp[0] */
                childLT(fkey(tmp[i])) = childLT(gkey);
                /* update LT, parent */
                childLT(pkey) = tmp[i]->adr;
            } else {
                /* update LT, tmp[i] */
                childLT(fkey(tmp[i])) = childGE(gkey);
                /* update parent key */
                memcpy(pkey, gkey, ks(1));
                childGE(pkey) = tmp[i]->adr;
                gkey += ks(1);
                pkey += ks(1);
                ct(tmp[i])--;
            }
        }

        /* install keys, tmp[i] */
        memcpy(fkey(tmp[i]), gkey, ks(ct(tmp[i])));
        leaf(tmp[i]) = leaf(gbuf);

        gkey += ks(ct(tmp[i]));
    }
    leaf(pbuf) = false;

    /************************
     * write modified nodes *
     ************************/
    if ((rc = writeDisk(pbuf)) != 0) return rc;
    for (i = 0; i < iu; i++)
        if ((rc = writeDisk(tmp[i])) != 0) return rc;

    return bOk;
}

static int gatherRoot(btFP *h) {
    btBuf *gbuf;
    btBuf *root;

    /* gather root to gbuf */
    root = &h->root;
    gbuf = &h->gbuf;
    memcpy(p(gbuf), root->p, 3 * h->sectorSize);
    leaf(gbuf) = leaf(root);
    ct(root) = 0;
    return bOk;
}

static int gather(btFP *h, btBuf *pbuf, btKey **pkey, btBuf **tmp) {
    int rc;                /* return code */
    btBuf *gbuf;
    btKey *gkey;

    /*
     * input:
     *   pbuf                   parent buffer
     *   pkey                   pointer to match key in parent
     * output:
     *   tmp                    buffers to use for scatter
     *   pkey                   pointer to match key in parent
     * returns:
     *   bOk    	        operation successful
     * notes:
     *   Gather 3 buffers to gbuf.  Setup for subsequent scatter by
     *   doing the following:
     *     - setup tmp buffer array for scattered buffers
     *     - adjust pkey to point to first key of 3 buffers
     */

    /* find 3 adjacent buffers */
    if (*pkey == lkey(pbuf))
        *pkey -= ks(1);
    if ((rc = readDisk(h, childLT(*pkey), &tmp[0])) != 0) return rc;
    if ((rc = readDisk(h, childGE(*pkey), &tmp[1])) != 0) return rc;
    if ((rc = readDisk(h, childGE(*pkey + ks(1)), &tmp[2])) != 0) return rc;

    /* gather nodes to gbuf */
    gbuf = &h->gbuf;
    gkey = fkey(gbuf);

    /* tmp[0] */
    childLT(gkey) = childLT(fkey(tmp[0]));
    memcpy(gkey, fkey(tmp[0]), ks(ct(tmp[0])));
    gkey += ks(ct(tmp[0]));
    ct(gbuf) = ct(tmp[0]);

    /* tmp[1] */
    if (!leaf(tmp[1])) {
        memcpy(gkey, *pkey, ks(1));
        childGE(gkey) = childLT(fkey(tmp[1]));
        ct(gbuf)++;
        gkey += ks(1);
    }
    memcpy(gkey, fkey(tmp[1]), ks(ct(tmp[1])));
    gkey += ks(ct(tmp[1]));
    ct(gbuf) += ct(tmp[1]);

    /* tmp[2] */
    if (!leaf(tmp[2])) {
        memcpy(gkey, *pkey+ks(1), ks(1));
        childGE(gkey) = childLT(fkey(tmp[2]));
        ct(gbuf)++;
        gkey += ks(1);
    }
    memcpy(gkey, fkey(tmp[2]), ks(ct(tmp[2])));
    ct(gbuf) += ct(tmp[2]);

    leaf(gbuf) = leaf(tmp[0]);

    return bOk;
}

btFP *btOpen(char *Name, int keySize, int dupKeys) {
    int rc;                /* return code */
    int bufCt;                  /* number of tmp buffers */
    btBuf *buf;               /* buffer */
    int maxCt;                  /* maximum number of keys in a node */
    btBuf *root;
    int i;
    btNode *p;
    btFP   *h;

    if (SECTORSIZE < sizeof(btNode)) return 0;

    /* determine sizes and offsets */
    /* leaf/n, prev, next, [childLT,key,rec]... childGE */
    /* ensure that there are at least 3 children/parent for gather/scatter */
    maxCt = SECTORSIZE - (sizeof(btNode) - sizeof(btKey));
    maxCt /= sizeof(bAdrType) + keySize + sizeof(eAdrType);
    if (maxCt < 6) return 0;

    /* copy parms to hNode */
    if ((h = (btFP *)malloc(sizeof(btFP))) == NULL) return 0;
    memset(h, 0, sizeof(btFP));
    h->keySize = keySize;
    h->dupKeys = dupKeys;
    h->sectorSize = SECTORSIZE;

    /* childLT, key, rec */
    h->ks = sizeof(bAdrType) + h->keySize + sizeof(eAdrType);
    h->maxCt = maxCt;

    /* Allocate buflist.
     * During insert/delete, need simultaneous access to 7 buffers:
     *  - 4 adjacent child bufs
     *  - 1 parent buf
     *  - 1 next sequential link
     *  - 1 lastGE
     */
    bufCt = 7;
    if ((h->malloc1 = malloc(bufCt * sizeof(btBuf))) == NULL)
        return 0;
    buf = h->malloc1;

    /*
     * Allocate bufs.
     * We need space for the following:
     *  - bufCt buffers, of size sectorSize
     *  - 1 buffer for root, of size 3*sectorSize
     *  - 1 buffer for gbuf, size 3*sectorsize + 2 extra keys
     *    to allow for LT pointers in last 2 nodes when gathering 3 full nodes
     */
    if ((h->malloc2 = malloc((bufCt+6) * h->sectorSize + 2 * h->ks)) == NULL)
        return 0;
    p = h->malloc2;

    /* initialize buflist */
    h->bufList.next = buf;
    h->bufList.prev = buf + (bufCt - 1);
    for (i = 0; i < bufCt; i++) {
        buf->next = buf + 1;
        buf->prev = buf - 1;
        buf->modified = false;
        buf->valid = false;
        buf->p = p;
        p = (btNode *)((char *)p + h->sectorSize);
        buf++;
    }
    h->bufList.next->prev = &h->bufList;
    h->bufList.prev->next = &h->bufList;

    /* initialize root */
    root = &h->root;
    root->p = p;
    p = (btNode *)((char *)p + 3*h->sectorSize);
    h->gbuf.p = p;      /* done last to include extra 2 keys */

    h->curBuf = NULL;
    h->curKey = NULL;

    /* initialize root */
    if ((h->fp = fopen(Name, "r+b")) != NULL) {
        /* open an existing database */
        if ((rc = readDisk(h, 0, &root)) != 0) return 0;
        if (fseek(h->fp, 0, SEEK_END)) return 0;
        if ((h->nextFreeAdr = ftell(h->fp)) == -1) return 0;
    } else if ((h->fp = fopen(Name, "w+b")) != NULL) {
        /* initialize root */
        memset(root->p, 0, 3*h->sectorSize);
        leaf(root) = 1;
        h->nextFreeAdr = 3 * h->sectorSize;
        root->modified = true;   /*HH: root was not written to disk */
    } else {
        /* something's wrong */
        free(h);
        return 0;
    }

    return h;
}

int btClose(btFP *h) {
    if (h == NULL) return bOk;

    /* flush idx */
    if (h->fp) {
        flushAll(h);
        fclose(h->fp);
    }

    if (h->malloc2) free(h->malloc2);
    if (h->malloc1) free(h->malloc1);
    free(h);
    return bOk;
}


int bFindKey_i(btFP *h, void *key, eAdrType krec, bool Mode, eAdrType *rec) {
    btKey *mkey;              /* matched key */
    btBuf *buf;               /* buffer */
    int   rc;                 /* return code */

    buf = &h->root;

    /* find key, and return address */
    while (1) {
        if (leaf(buf)) {
            rc=search(h, buf, key, krec, &mkey, Mode, 1);
            if (rec) *rec = rec(mkey);
            if (key) memcpy(key, key(mkey), h->keySize);

            if (rc==1) return bErrEndOfIndex;
            if (rc && Mode==MODE_MATCH) return bErrKeyNotFound;
            if (rc) return bHigherKeyFound;
            return bKeyFound;
        } else {
            if (search(h, buf, key, krec, &mkey, Mode, 0) < 0) {
                if ((rc = readDisk(h, childLT(mkey), &buf)) != 0) return rc;
            } else {
                if ((rc = readDisk(h, childGE(mkey), &buf)) != 0) return rc;
            }
        }
    }
}

int btInsertKey(btFP *h, void *key, eAdrType rec) {
    int rc;                     /* return code */
    btKey *mkey;              /* match key */
    int len;                    /* length to shift */
    int cc;                     /* condition code */
    btBuf *buf, *root;
    btBuf *tmp[4];
    unsigned int keyOff;
    bool lastGEvalid;           /* true if GE branch taken */
    bool lastLTvalid;           /* true if LT branch taken after GE branch */
    bAdrType lastGE;            /* last childGE traversed */
    unsigned int lastGEkey;     /* last childGE key traversed */
    int height;                 /* height of tree */

    root = &h->root;
    lastGEvalid = false;
    lastLTvalid = false;
    lastGE = (bAdrType)0;
    lastGEkey = 0;

    /* check for full root */
    if (ct(root) == 3 * h->maxCt) {
        /* gather root and scatter to 4 bufs */
        /* this increases b-tree height by 1 */
        if ((rc = gatherRoot(h)) != 0) return rc;
        if ((rc = scatter(h, root, fkey(root), 0, tmp)) != 0) return rc;
    }
    buf = root;
    height = 0;
    while (1) {
        if (leaf(buf)) {
            /* in leaf, and there' room guaranteed */

            /* set mkey to point to insertion point */
            switch (search(h, buf, key, rec, &mkey, MODE_MATCH,0)) {
            case CC_LT:  /* key < mkey */
                if (ct(buf) && h->dupKeys==0
                        && memcmp(key,mkey,h->keySize) == CC_EQ)
                    return bErrDupKeys;
                break;
            case CC_EQ:  /* key = mkey */
                return bErrDupKeys;
                break;
            case CC_GT:  /* key > mkey */
                if (h->dupKeys==0 && memcmp(key,mkey,h->keySize) == CC_EQ)
                    return bErrDupKeys;
                mkey += ks(1);
                break;
            }

            /* shift items GE key to right */
            keyOff = mkey - fkey(buf);
            len = ks(ct(buf)) - keyOff;
            if (len) memmove(mkey + ks(1), mkey, len);

            /* insert new key */
            memcpy(key(mkey), key, h->keySize);
            rec(mkey) = rec;
            childGE(mkey) = 0;
            ct(buf)++;
            if ((rc = writeDisk(buf)) != 0) return rc;

            /* if new key is first key, then fixup lastGE key */
            if (!keyOff && lastLTvalid) {
                btBuf *tbuf;
                btKey *tkey;
                if ((rc = readDisk(h, lastGE, &tbuf)) != 0) return rc;
                tkey = fkey(tbuf) + lastGEkey;
                memcpy(key(tkey), key, h->keySize);
                rec(tkey) = rec;
                if ((rc = writeDisk(tbuf)) != 0) return rc;
            }
            break;
        } else {
            /* internal node, descend to child */
            btBuf *cbuf;      /* child buf */

            height++;

            /* read child */
            if ((cc = search(h, buf, key, rec, &mkey, MODE_MATCH,0)) < 0) {
                if ((rc = readDisk(h, childLT(mkey), &cbuf)) != 0) return rc;
            } else {
                if ((rc = readDisk(h, childGE(mkey), &cbuf)) != 0) return rc;
            }

            /* check for room in child */
            if (ct(cbuf) == h->maxCt) {

                /* gather 3 bufs and scatter */
                if ((rc = gather(h, buf, &mkey, tmp)) != 0) return rc;
                if ((rc = scatter(h, buf, mkey, 3, tmp)) != 0) return rc;

                /* read child */
                if ((cc = search(h, buf, key, rec, &mkey, MODE_MATCH,0)) < 0) {
                    if ((rc = readDisk(h, childLT(mkey), &cbuf)) != 0) return rc;
                } else {
                    if ((rc = readDisk(h, childGE(mkey), &cbuf)) != 0) return rc;
                }
            }
            if (cc >= 0 || mkey != fkey(buf)) {
                lastGEvalid = true;
                lastLTvalid = false;
                lastGE = buf->adr;
                lastGEkey = mkey - fkey(buf);
                if (cc < 0) lastGEkey -= ks(1);
            } else {
                if (lastGEvalid) lastLTvalid = true;
            }
            buf = cbuf;
        }
    }

    return bOk;
}

int btDeleteKey(btFP *h, void *key, eAdrType rec) {
    int rc;                     /* return code */
    btKey *mkey;              /* match key */
    int len;                    /* length to shift */
    int cc;                     /* condition code */
    btBuf *buf;               /* buffer */
    btBuf *tmp[4];
    unsigned int keyOff;
    bool lastGEvalid;           /* true if GE branch taken */
    bool lastLTvalid;           /* true if LT branch taken after GE branch */
    bAdrType lastGE;            /* last childGE traversed */
    unsigned int lastGEkey;     /* last childGE key traversed */
    btBuf *root;
    btBuf *gbuf;

    root = &h->root;
    gbuf = &h->gbuf;
    lastGEvalid = false;
    lastLTvalid = false;

    lastGE = (bAdrType)0;
    lastGEkey = 0;
    buf = root;
    while (1) {
        if (leaf(buf)) {

            /* set mkey to point to deletion point */
            if (search(h, buf, key, rec, &mkey, MODE_MATCH,0) != 0)
                return bErrKeyNotFound;

            /* shift items GT key to left */
            keyOff = mkey - fkey(buf);
            len = ks(ct(buf)-1) - keyOff;
            if (len) memmove(mkey, mkey + ks(1), len);
            ct(buf)--;
            if ((rc = writeDisk(buf)) != 0) return rc;

            /* if deleted key is first key, then fixup lastGE key */
            if (!keyOff && lastLTvalid) {
                btBuf *tbuf;
                btKey *tkey;
                if ((rc = readDisk(h,lastGE, &tbuf)) != 0) return rc;
                tkey = fkey(tbuf) + lastGEkey;
                memcpy(key(tkey), mkey, h->keySize);
                rec(tkey) = rec(mkey);
                if ((rc = writeDisk(tbuf)) != 0) return rc;
            }
            break;
        } else {
            /* internal node, descend to child */
            btBuf *cbuf;      /* child buf */

            /* read child */
            if ((cc = search(h,buf, key, rec, &mkey, MODE_MATCH,0)) < 0) {
                if ((rc = readDisk(h,childLT(mkey), &cbuf)) != 0) return rc;
            } else {
                if ((rc = readDisk(h,childGE(mkey), &cbuf)) != 0) return rc;
            }

            /* check for room to delete */
            if (ct(cbuf) == h->maxCt/2) {

                /* gather 3 bufs and scatter */
                if ((rc = gather(h, buf, &mkey, tmp)) != 0) return rc;

                /* if last 3 bufs in root, and count is low enough... */
                if (buf == root
                        && ct(root) == 2
                        && ct(gbuf) < (3*(3*h->maxCt))/4) {
                    /* collapse tree by one level */
                    scatterRoot(h);
                    continue;
                }

                if ((rc = scatter(h,buf, mkey, 3, tmp)) != 0) return rc;

                /* read child */
                if ((cc = search(h, buf, key, rec, &mkey, MODE_MATCH,0)) < 0) {
                    if ((rc = readDisk(h, childLT(mkey), &cbuf)) != 0) return rc;
                } else {
                    if ((rc = readDisk(h, childGE(mkey), &cbuf)) != 0) return rc;
                }
            }
            if (cc >= 0 || mkey != fkey(buf)) {
                lastGEvalid = true;
                lastLTvalid = false;
                lastGE = buf->adr;
                lastGEkey = mkey - fkey(buf);
                if (cc < 0) lastGEkey -= ks(1);
            } else {
                if (lastGEvalid) lastLTvalid = true;
            }
            buf = cbuf;
        }
    }

    return bOk;
}

int btFindFirstKey(btFP *h, void *key, eAdrType *rec) {
    int rc;                /* return code */
    btBuf *buf;              /* buffer */

    buf = &h->root;
    while (!leaf(buf)) {
        if ((rc = readDisk(h, childLT(fkey(buf)), &buf)) != 0) return rc;
    }
    if (ct(buf) == 0) return bErrEndOfIndex;
    if (key) memcpy(key, fkey(buf), h->keySize);
    if (rec) *rec = rec(fkey(buf));
    h->curBuf = buf;
    h->curKey = fkey(buf);
    return bKeyFound;
}

int btFindLastKey(btFP *h, void *key, eAdrType *rec) {
    int rc;                /* return code */
    btBuf *buf;               /* buffer */

    buf = &h->root;
    while (!leaf(buf)) {
        if ((rc = readDisk(h, childGE(lkey(buf)), &buf)) != 0) return rc;
    }
    if (ct(buf) == 0) return bErrEndOfIndex;
    if (key) memcpy(key, lkey(buf), h->keySize);
    if (rec) *rec = rec(lkey(buf));
    h->curBuf = buf;
    h->curKey = lkey(buf);
    return bKeyFound;
}

int btFindNextKey(btFP *h, void *key, eAdrType *rec) {
    int rc;                /* return code */
    btKey *nkey;             /* next key */
    btBuf *buf;              /* buffer */

    if ((buf = h->curBuf) == NULL) return bErrKeyNotFound;
    if (h->curKey == lkey(buf)) {
        /* current key is last key in leaf node */
        if (next(buf)) {
            /* fetch next set */
            if ((rc = readDisk(h, next(buf), &buf)) != 0) return rc;
            nkey = fkey(buf);
        } else {
            /* no more sets */
            return bErrEndOfIndex;
        }
    } else {
        /* bump to next key */
        nkey = h->curKey + ks(1);
    }
    if (key) memcpy(key, key(nkey), h->keySize);
    if (rec) *rec = rec(nkey);
    h->curBuf = buf;
    h->curKey = nkey;
    return bKeyFound;
}

int btFindPrevKey(btFP *h, void *key, eAdrType *rec) {
    int rc;                /* return code */
    btKey *pkey;             /* previous key */
    btKey *fkey;             /* first key */
    btBuf *buf;              /* buffer */

    if ((buf = h->curBuf) == NULL) return bErrKeyNotFound;
    fkey = fkey(buf);
    if (h->curKey == fkey) {
        /* current key is first key in leaf node */
        if (prev(buf)) {
            /* fetch previous set */
            if ((rc = readDisk(h, prev(buf), &buf)) != 0) return rc;
            pkey = fkey(buf) + ks((ct(buf) - 1));
        } else {
            /* no more sets */
            return bErrEndOfIndex;
        }
    } else {
        /* bump to previous key */
        pkey = h->curKey - ks(1);
    }
    if (key) memcpy(key, key(pkey), h->keySize);
    if (rec) *rec = rec(pkey);
    h->curBuf = buf;
    h->curKey = pkey;
    return bKeyFound;
}
