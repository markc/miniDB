/* -------------------------------------------------------------------------- *
 * Description  : miniDB interface
 *
 * Revision 2.1 : added cmd for rebuildindex function
 * Revision 2.0 : Adapted for new mdb_lib functions
 * Revision 1.0 : Initial revision
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
#include <getopt.h>
#include "mdb_lib.h"
#include "mdb_func.h"

int 	NOROWS=1;		/* no rows to retrieve */


void syntax() {
    puts("Syntax: mdb [-b<idstring>] [-r<norows>] [-k<keyno>] [-h <host>] [-p <port>]");
    puts("                           <tablename> <cmd> [<key> [<data>]]");
    puts(" Options:  -b  assign a stringid for recognising empty fields");
    puts("               used when updating a record");
    puts("           -f  show field names before data");
    puts("           -k  select key");
    puts("           -r  retrieve .. number of rows");
    puts("           -h  hostname (only in case of client/server)");
    puts("           -p  portno (default 7221)");
    puts("           -u  used fields");
    puts("");
    puts("       cmd = f,l   (first,last)");
    puts("             p,e,n (previous, equal, next)");
    puts("             a,u,d (add, update, delete)");
    puts("	     s     (search)");
    puts("	     r     (retrieve record 1..n)");
    puts("	     B,C,D (reBuild, create or drop index)");
    puts("	     L     (database logging on/off)");
    puts(" Examples: ");
    puts("   mdb customer c 'custid:n6|seak:c8|Name:a30|place:a30|_idx:1|_idx:2:1|' ");
    puts("   mdb customer a '1201|doe|John Doe|New York|' ");
    puts("   mdb customer e '1201' ");
    puts("   mdb -k2 -r10 customer s 'do' ");
    puts("   mdb -b'#' customer u '1201' '#|#|London|' ");
    puts("   mdb -k2 customer D  ");
    puts("   mdb -k2 customer C '2:1' ");
    exit(1);
}

void do_error(char *s) {
    puts(s);
    exit(1);
}

/*
 * rearrange supplied data in the correct recordlayout
 * ex. supplied data and layout : "Doe|John|1234"  "lastnm|firstnm|id"
 *     recordlayout       : "id|firstnm|lastnm|place|country"
 *     rearranged output  : "1234|John|Doe|#|#"
 */
void rebuilddata(src,uflds,flds,deflt,dst)
char *src,*uflds,*flds,*deflt,*dst;
{
    char fldval[257],tmp[61];
    int  i,j;

    dst[0]=0;
    /* for each recordfield, find a supplied value
     * skip first field in recorddef, because its the recno */
    for (j=2; getfield(j,flds,fldval,60,'|')!=0; j++) {
        if (fldval[0]==0) break;
        /* check if supplied by user */
        for (i=1; getfield(i,uflds,tmp,60,'|')!=0; i++)
            if (tmp[0]==0 || strcmp(fldval,tmp)==0)  break;
        if (tmp[0]) {	/* if found, fill with the corresponding data */
            getfield(i,src,fldval,256,'|');
            strcat(dst,fldval);
        } else		/* else use the default value */
            strcat(dst,deflt);
        strcat(dst,"|");
    }
}

int addrecord(fn,data,dataflds,fldnms)
char *fn,*data,*dataflds,*fldnms;
{
    if (dataflds[0]) {
        char dta[513];
        rebuilddata(data,dataflds,fldnms,"",dta);
        return mDB_add(fn,dta,0);
    } else
        return mDB_add(fn,data,0);
}

int  main(argc, argv)
int  argc;
char *argv[];
{
    char fn[81];
    char keystr[513],fldnms[256];
    char EMPTYID[11]="#";
    char UPDFLDS[256];
    int  FIELDS, st,ch, keylen, idxno;

    FIELDS=0;
    idxno=1;
    st=0;
    UPDFLDS[0]=0;
    while ((ch=getopt(argc,argv,"b:fh:p:k:r:u:"))!=-1) {
        switch (ch) {
        case 'b' :
            strcpy(EMPTYID,optarg);
            break;
        case 'f' :
            FIELDS=1;
            break;
        case 'k' :
            idxno=atoi(optarg);
            break;
        case 'r' :
            NOROWS=atoi(optarg);
            break;
        case 'h' :
            strcpy(mDBhostname,optarg);
            break;
        case 'p' :
            mDBportno=atoi(optarg);
            break;
        case 'u' :
            strcpy(UPDFLDS,optarg);
            break;
        default  :
            syntax();
            break;
        }
    }
    argc -=optind;
    argv +=optind;
    if (argc<2)  syntax();

    if (mDB_init()==0) {
        puts("Database initialisation unsuccessfull !");
        exit(-1);
    }

    sprintf(fn,"data/%s",argv[0]);
    strcpy(keystr,(argc>2)?argv[2]:"");
    keylen=strlen(keystr);
    if (strchr("ck",argv[1][0])) {
        switch (argv[1][0]) {
        case 'c' :
            if (mDB_create(fn,keystr)==0) do_error("create error");
            break;
        case 'k' :
            if (mDB_kill(argv[0])==0) do_error("access denied");
            break;
        }
    } else {
        if (mDB_open(fn)==0) {
            printf("Unknown databasefile [%s]\n",fn);
            exit(-2);
        }
        mDB_fields(fn,fldnms);
        if (FIELDS) puts(fldnms);
        switch (argv[1][0]) {
        case 'a' :
            if (strcmp(keystr,"-")==0) {
                while (fgets(keystr,512,stdin)!=NULL) {
                    strtrim(keystr);
                    if (keystr[0])
                        st=addrecord(fn,keystr,UPDFLDS,fldnms);
                }
            } else
                st=addrecord(fn,keystr,UPDFLDS,fldnms);
            break;
        case 'C' :
            st=mDB_createindex(fn,idxno,keystr);
            break;
        case 'd' :
            st=mDB_delete(fn,idxno,keystr);
            break;
        case 'e' :
            st=mDB_find(fn,idxno,  keystr,0);
            break;
        case 'f' :
            st=mDB_first(fn,idxno,NOROWS,0);
            break;
        case 'i' :
            st=mDB_header(fn,0,0);
            break;
        case 'l' :
            st=mDB_last (fn,idxno,NOROWS,0);
            break;
        case 'n' :
            st=mDB_next(fn,idxno,  keystr,NOROWS,0);
            break;
        case 'p' :
            st=mDB_prev(fn,idxno,  keystr,NOROWS,0);
            break;
        case 'r' :
            st=mDB_read(fn,(UL)atol(keystr),0);
            break;
        case 's' :
            st=mDB_search(fn,idxno,keystr,NOROWS,0);
            break;
        case 'u' :
            if (argc>3) {
                if (UPDFLDS[0]) {
                    char dta[513];
                    rebuilddata(argv[3],UPDFLDS,fldnms,EMPTYID,dta);
                    st=mDB_update(fn,idxno,keystr,EMPTYID,dta,0);
                } else
                    st=mDB_update(fn,idxno,keystr,EMPTYID,argv[3],0);
            }
            break;
        case 'B' :
            st=mDB_rebuildindex(fn,idxno);
            break;
        case 'D' :
            st=mDB_dropindex(fn,idxno);
            break;
        case 'L' :
            st=mDB_log(fn,keystr);
            break;
        default  :
            printf("unknown cmd [%c]\n",argv[1][0]);
            syntax();
            break;
        }
        if (st!=1) printf("Error %d\n",st);
        mDB_close(fn);
    }
    mDB_exit();
    exit(st);
}

