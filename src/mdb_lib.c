/* -------------------------------------------------------------------------- *
 * Description  : ISAM library, data and btree file wrapper
 *
 * Revision 2.1 : Removed old btree library references
 *                Added rebuild index function (can be used to quickly migrate to 2.xx)
 * Revision 2.0 : Adapted for new btree library
 *                Redesigned functions for better linking to programs
 * Revision 1.0 : Initial revision
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
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include "bt.h"
#include "mdb_lib.h"
#include "mdb_func.h"

#define   mDBVERSION "ISBT2.0"		 /* version of datafile layout */
mDBF	  *mDBFS = NULL;   	         /* Linklist off open database files */
mDBOUTPUT *mDB_output=mDBi_showoutput;
char	  mDBhostname[41]="127.0.0.1";
int	  mDBportno=7221;

void mDBi_showoutput(mDBF *mDB, char *s) {
    if (mDB)
        printf("%lu|%s\n",mDB->recno,s);
    else
        printf("%s\n",s);
}

int mDBi_writehdr(mDB)
mDBF *mDB;
{
    char  hdr[256],tmp[9];
    int	i,j;

    sprintf(hdr,"%s|%d|%d|%ld|%ld|%ld|%ld|%ld|%d|%d|%d|%d|",
            mDB->ID,
            mDB->recsize ,mDB->datastart,
            mDB->creation,mDB->updtime,
            mDB->delno   ,mDB->dellist  ,mDB->autoincr,mDB->logging,
            mDB->fldno   ,mDB->fldbytes ,mDB->keyno);
    for (i=0; i<MDB_KEYMAX; i++) {
        if (mDB->key[i][0]!=0) {
            for (j=0; j<8; j++)
                if (mDB->key[i][j]!=0) {
                    sprintf(tmp,"%s%d",(j)?":":"",mDB->key[i][j]);
                    strcat(hdr,tmp);
                }
        }
        strcat(hdr,"|");
    }
    i=strlen(hdr);
    memset(&hdr[i],32,255-i);			/* make remaining empty */
    hdr[255]=10;
    if (fseek(mDB->fp,0,SEEK_SET)!=0)  return(0);
    i=fwrite(hdr,256,1,mDB->fp);
    fflush(mDB->fp);
    return i;
}


int mDBi_readhdr(mDB,version_sw)
mDBF *mDB;
int version_sw;
{
    char hdr[256],fld[41],*p;
    int  i,n;

    if (fseek(mDB->fp,0,SEEK_SET)!=0)  return(0);
    if (fread(hdr,256,1,mDB->fp) !=1)  return(0);
    if ((!version_sw && strncmp(hdr,mDBVERSION,7)!=0) || hdr[255]!=10) return(0);

    /* ------------- clear key list --------------- */
    for (i=0; i<MDB_KEYMAX; i++)  for (n=0; n<8; n++) mDB->key[i][n]=0;

    for (i=1; i<21; i++) {
        getfield(i,hdr,fld,40,'|');
        switch (i) {
        case  1 :
            fld[7]=0;
            strcpy(mDB->ID,fld);
            break;
        case  2 :
            mDB->recsize=(unsigned)atoi(fld);
            break;
        case  3 :
            mDB->datastart=(unsigned)atoi(fld);
            break;
        case  4 :
            mDB->creation=(time_t)atol(fld);
            break;
        case  5 :
            mDB->updtime=(time_t)atol(fld);
            break;
        case  6 :
            mDB->delno=(UL)atol(fld);
            break;
        case  7 :
            mDB->dellist=(UL)atol(fld);
            break;
        case  8 :
            mDB->autoincr=(unsigned)atoi(fld);
            break;
        case  9 :
            mDB->logging=atoi(fld);
            break;
        case 10 :
            mDB->fldno=atoi(fld);
            break;
        case 11 :
            mDB->fldbytes=atoi(fld);
            break;
        case 12 :
            mDB->keyno=atoi(fld);
            break;
        default :
            if (i<22) {
                p=strtok(fld,":");
                for (n=0; (p && n<8); n++) {
                    mDB->key[i-13][n]=atoi(p);
                    p=strtok(NULL,":");
                }
            }
            break;
        }
    }
    return(1);
}

/* check if fn is already opened */
mDBF *mDBi_fn(fn)
char *fn;
{
    mDBF *cur;

    cur = mDBFS;
    while (cur!=NULL && strcmp(fn,cur->fn)!=0)  cur=cur->next;
    return cur;
}

/* add new fn structure */
mDBF *mDBi_addfn(fn)
char *fn;
{
    mDBF *cur,*new;

    cur=mDBFS;
    new=(mDBF *)malloc(sizeof(mDBF));
    if (new) {
        new->next=cur;
        new->prev=0;
        if (cur)  cur->prev=new;
        strcpy(new->fn,fn);
        new->no=1;
        mDBFS=new;
    }
    return new;
}

/* add new fn structure */
void mDBi_delfn(cur)
mDBF *cur;
{
    mDBF *tmp;

    tmp=cur->prev;
    if (cur->prev) {
        tmp=cur->prev;
        tmp->next=cur->next;
    } else
        mDBFS=cur->next;
    if (cur->next) {
        tmp=cur->next;
        tmp->prev=cur->prev;
    }
    free(cur);    /* and the rest */
}


mDBF *mDBi_open(fn,version_sw)
char *fn;
int  version_sw;
{
    mDBF   *cur;
    char 	*p,*n,*s,fn2[81],*flds;
    int	len,i,ofs;

    cur=mDBi_fn(fn);
    if (cur) {
        if (version_sw || strcmp(cur->ID,mDBVERSION)==0) return(cur);
        return(0);
    }
    if ((cur=mDBi_addfn(fn))==0) return(0);
    sprintf(fn2,"%s.dat",fn);
    if ((cur->fp=fopen(fn2,"rb+"))==0) {
        mDBi_delfn(cur);
        return 0;
    }
    /* --------- read fileinfo ---------------- */
    if (mDBi_readhdr(cur,version_sw)==0) {
        mDBi_delfn(cur);
        return(0);
    }
    len=cur->fldbytes;
    flds=cur->fldnms=(char *)malloc(len);
    if (fread(flds,len,1,cur->fp)==0) {
        mDBi_delfn(cur);
        return(0);
    }
    flds[len]=0;		/* skip <LF> */
    strcpy(&flds[0],&flds[1]);	/* skip STATUS char */

    fseek(cur->fp,0,SEEK_END);
    cur->records= (ftell(cur->fp) - cur->datastart) / cur->recsize;

    cur->fldno  = cur->fldno;
    cur->fld    =(char **)malloc( sizeof(char *) * cur->fldno);
    cur->fldtype=(char **)malloc( sizeof(char *) * cur->fldno);
    cur->fldofs =(int *)  malloc( sizeof(int   ) * cur->fldno);
    cur->record =(char *) malloc( sizeof(char *) * cur->recsize);
    cur->varrec =(char *) malloc((sizeof(char *) * cur->recsize)+cur->fldno+1);

    if (cur->logging) {
        sprintf(fn2,"%s.log",fn);
        cur->logfp=fopen(fn2,"a+");
    }
    /* --------------- build fieldname array ------------ */
    i=0;
    ofs=1;
    for (p=flds;(n=strchr(p,'|'))!=0; p=n) {
        n[0]=0;
        cur->fld[i]=p;
        s=strchr(p,':');
        s[0]=0;
        cur->fldtype[i]=&s[1];
        cur->fldofs[i]=ofs;
        ofs+=atoi(&s[2]);
        n++;
        i++;
    }
    return(cur);
}

int mDBi_keylength(mDB,k)
mDBF *mDB;
int  k;
{
    int l,j,n,fldno;

    l=0;
    if (k>=0 && k<8 && mDB->key[k][0]!=0) {
        for (j=0; j<8; j++) {
            fldno=mDB->key[k][j];
            if (fldno) {
                if (l) l++;
                n=atoi(&mDB->fldtype[fldno-1][1]);
                l+=n;
            }
        }
    }
    return l;
}

int mDB_open(fn)
char *fn;
{
    mDBF *mDB;
    mDB=mDBi_open(fn,1);
    if (mDB) {
        mDB->no++; 			/* increase number connections */
        return(1);
    }
    return(0);
}

int mDBi_close(mDB)
mDBF *mDB;
{
    mDBi_writehdr(mDB);			/* save isam header */
    if (mDB->no) mDB->no--; 		/* decrease number connections */
    if (mDB->no) return(1);
    mDBi_idxclose(mDB,0);			/* close all open indexes      */
    if (mDB->logging && mDB->logfp) fclose(mDB->logfp);
    fclose(mDB->fp);
    mDBi_delfn(mDB);			/* remove from open file list  */
    return(1);
}

int mDB_close(char *fn) {
    mDBF *mDB;

    mDB=mDBi_fn(fn);
    if (!mDB) return(0);
    return mDBi_close(mDB);
}

int mDB_fields(fn,dst)
char *fn, *dst;
{
    mDBF    *mDB;
    int     i;
    char    str[512],tmp[81];

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    strcpy(str,"recno|");
    for (i=0; i<mDB->fldno; i++) {
        sprintf(tmp,"%s|",mDB->fld[i]);
        strcat(str,tmp);
    }
    if (dst) strcpy(dst,str);
    else if (mDB_output) mDB_output(0,str);
    return(1);
}

int mDB_header(fn,type,dst)
char *fn,*dst;
int  type;
{
    mDBF   *mDB;
    int	 tp,i,j,fld;
    struct tm *t;
    char   str[512],tmp[81];

    if ((mDB=mDBi_open(fn,1))==0) return(0);  /* diff versions allowed */

    for (tp=MDB_DATES; tp<=MDB_FIELDS; tp++) {
        if (type==0 || tp==type) {
            fld=0;
            switch (tp) {
            case MDB_DATES :
                t=localtime(&mDB->creation);
                sprintf(str,"version  =%s|",mDB->ID);
                sprintf(tmp,"creation =%04d-%02d-%02d %02d:%02d:%02d|",
                        1900+t->tm_year,t->tm_mon+1,t->tm_mday,
                        t->tm_hour,t->tm_min,t->tm_sec);
                strcat(str,tmp);
                t=localtime(&mDB->updtime);
                sprintf(tmp,"updated  =%04d-%02d-%02d %02d:%02d:%02d|",
                        1900+t->tm_year,t->tm_mon+1,t->tm_mday,
                        t->tm_hour,t->tm_min,t->tm_sec);
                strcat(str,tmp);
                fld=2;
                break;
            case MDB_RECORD:
                sprintf(str,"recsize  =%d|datastart=%d|autoincr =%ld|logging  =%s|records  =%lu|no.del   =%lu|dellist  =%lu|no.fld   =%d|no.key   =%d|",
                        mDB->recsize,mDB->datastart,mDB->autoincr,
                        mDB->logging?"on":"off",mDB->records - mDB->delno,
                        mDB->delno,mDB->dellist,mDB->fldno,mDB->keyno);
                fld=9;
                break;
            case MDB_CREATE:
                sprintf(str,"create   =");
                for (i=0; i<mDB->fldno; i++) {
                    sprintf(tmp,"%s:%s|",mDB->fld[i],mDB->fldtype[i]);
                    strcat(str,tmp);
                }
                for (i=0; i<MDB_KEYMAX; i++) {
                    if (mDB->key[i][0]!=0) {
                        sprintf(tmp,"_key%d",i+1);
                        strcat(str,tmp);
                        for (j=0; j<8; j++) if (mDB->key[i][j]!=0) {
                                sprintf(tmp,":%d",mDB->key[i][j]);
                                strcat(str,tmp);
                            }
                        strcat(str,"|");
                    }
                }
                fld=1;
                break;
            case MDB_FIELDS:
                strcpy(str,"fldnames =");
                for (i=0; i<mDB->fldno; i++) {
                    sprintf(tmp,"%s|",mDB->fld[i]);
                    strcat(str,tmp);
                }
                fld=1;
                break;
            default    :
                return(0);
                break;
            } /*switch*/
            if (fld) {
                if (dst)
                    strcpy(dst,str);
                else if (mDB_output) {
                    if (fld==1)
                        mDB_output(0,str);
                    else {
                        for (i=1; i<=fld; i++) {
                            getfield(i,str,tmp,80,'|');
                            mDB_output(0,tmp);
                        }
                    }
                }
            }
        }
    } /*for*/
    return(1);
}

int mDB_create(fn,flds)
char *fn;
char *flds;
{
    mDBF	  mDB, *mDBp;
    btFP 	  *IDX;
    char 	  *p,*n,*s,fn2[81],nms[512];
    int	  i,l;

    memset(&mDB, 0, sizeof(mDB));
    strncpy(mDB.ID,mDBVERSION,7);
    strcpy(nms,"F");
    for (p=flds;(n=strchr(p,'|'))!=0; p=n) {
        n[0]=0;
        if ((s=strchr(p,':'))==0) return(0);
        if (*p=='_') {
            i=0;
            do {
                s[0]=0;
                p=&s[1];
                mDB.key[mDB.keyno][i++]=atoi(p);
                s=strchr(p,':');
            } while (s);
            mDB.keyno++;
        } else {
            mDB.fldno++;
            strcat(nms,p);
            strcat(nms,"|");
            mDB.recsize+=atoi(&s[2]);
        }
        n++;
    }
    mDB.recsize+=mDB.fldno+2;	/* increase for <status>[<record..>]<LF>
				   and fieldseperators */
    mDB.fldbytes=strlen(nms)+1;	/* len = <status><flds...> + <LF> */
    nms[mDB.fldbytes-1]=10;
    mDB.datastart=256+mDB.fldbytes;
    mDB.creation=mDB.updtime=time(0);
    strcpy(mDB.fn,fn);
    /* -------------------------- create datafile(s) */
    sprintf(fn2,"%s.dat",fn);
    if ((mDB.fp=fopen(fn2,"wb"))==0) return(0);
    mDBi_writehdr(&mDB);
    /* --------------------------- Fieldnames record*/
    fwrite(nms,mDB.fldbytes,1,mDB.fp);
    fclose(mDB.fp);
    mDBp=mDBi_open(fn,0);
    if (mDBp) {
        /* ------------------------- create indexfile(s) */
        for (i=0; i<mDB.keyno; i++) {
            sprintf(fn2,"%s.idx%d",fn,i+1);
            l=mDBi_keylength(mDBp,i);
            if (l) {
                unlink(fn2);
                IDX=btOpen(fn2, l, 1);
                if (IDX==0) return(0);
                btClose(IDX);
            }
        }
        mDBi_close(mDBp);
    }
    return(1);
}

void mDBi_fieldfmt(mDB,format,keyfld,dst)
mDBF  *mDB;
char *format;
char *keyfld,*dst;
{
    char   *p,t;
    int    i,l,l2;
    double f;

    t=format[0];
    l=atoi(&format[1]);
    p=strchr(&format[1],'.');
    l2=(p)?atoi(&p[1]):0;
    if (!dst && t=='c') t='a';
    switch (t) {
    case 'c' :		/* only UPPERCASE and without spaces */
        strtrim(keyfld);
        while ((p=strchr(keyfld,' '))!=0) strcpy(&p[0],&p[1]);
        for (i=0; keyfld[i]!=0; i++)  keyfld[i]=toupper(keyfld[i]);
        break;
    case 'a' :
        while (strlen(keyfld)<l) strcat(keyfld," ");
        break;
    case 'n' :  	/* numeric key */
        i=atoi(keyfld);
        sprintf(keyfld,"%0*d",l,i);
        break;
    case 'i' :  	/* numeric key */
        i=atoi(keyfld);
        if (mDB) {
            if (i==0) {
                mDB->autoincr++;
                i=mDB->autoincr;
            } else if (mDB->autoincr<i)
                mDB->autoincr=i;
        }
        sprintf(keyfld,"%0*d",l,i);
        break;
    case 'f' :  	/* floatingpoint key */
        f=atof(keyfld);			/* to be improved */
        sprintf(keyfld,"%0*.*f",l,l2,f);
        break;
    }
    if (dst) {
        strtrim(keyfld);
        if (strlen(keyfld)<l) strcat(keyfld," ");
        if (*dst!=0)  strcat(dst,"|");    /* place seperator between keyfields */
        strcat(dst,keyfld);
    }
}

void mDBi_rec2field(mDB,fldno)
mDBF *mDB;
int fldno;
{
    char   *p,t, fldstr[81];
    int    l,l2;
    double f;

    t=mDB->fldtype[fldno][0];
    l=atoi(&mDB->fldtype[fldno][1]);
    p=strchr(&mDB->fldtype[fldno][1],'.');
    l2=(p)?atoi(&p[1]):0;
    strncpy(fldstr,&mDB->record[mDB->fldofs[fldno]],l);
    fldstr[l]=0;
    strtrim(fldstr);
    switch (t) {
    case 'c' :
    case 'a' :
        break;
    case 'i' :
    case 'n' :  	/* numeric fld */
        l=atoi(fldstr);
        sprintf(fldstr,"%d",l);
        break;
    case 'f' :  	/* floatingpoint key */
        f=atof(fldstr);	 		   /* to be improved */
        sprintf(fldstr,"%*.*f",l2+2,l2,f);
        break;
    }
    /* place seperator between keyfields */
    strcat(mDB->varrec,fldstr);
    strcat(mDB->varrec,"|");
}

void mDBi_rec2fields(mDB)
mDBF *mDB;
{
    int i;
    strcpy(mDB->varrec,"");
    for (i=0; i<mDB->fldno; i++) mDBi_rec2field(mDB,i);
}


void mDBi_field2rec(mDB,clr,blank,data)
mDBF  *mDB;
int  clr;
char *blank,*data;
{
    int  		i,l;
    char 		fldstr[81];

    if (clr) memset(mDB->record,32,mDB->recsize);   	/* clear record */
    mDB->record[0]='R';                                /* valid Row status */
    for (i=0; i<mDB->fldno; i++) {
        l=atoi(&mDB->fldtype[i][1]);
        getfield(i+1,data,fldstr,l,'|');
        if (clr || (blank && strcmp(blank,fldstr)!=0)) {	 /* only if filled */
            mDBi_fieldfmt(mDB,mDB->fldtype[i],fldstr,0);
            strncpy(&mDB->record[mDB->fldofs[i]],fldstr,l);
        }
    }
    mDB->record[mDB->recsize-1]=10;
}


void mDBi_buildkey(mDB,no,key)
mDBF  *mDB;
int  no;
char *key;
{
    char  t,keystr[81];
    int   l,i,fldno;

    strcpy(key,"");
    for (i=0; i<8; i++) {
        fldno=mDB->key[no][i];
        if (fldno==0) break;
        fldno--;
        t=mDB->fldtype[fldno][0];
        l=atoi(&mDB->fldtype[fldno][1]);
        strncpy(keystr,&mDB->record[mDB->fldofs[fldno]],l);
        keystr[l]=0;
        mDBi_fieldfmt(mDB,mDB->fldtype[fldno],keystr,key);
    }
}

void mDBi_updkey(mDB,type,recno)
mDBF  *mDB;
char type;
UL   recno;
{
    int  i;
    char key[80];

    mDBi_idxopen(mDB,0);
    for (i=0; i<MDB_KEYMAX; i++) {
        if (mDB->idx[i]!=0) {
            mDBi_buildkey(mDB,i,key);
            if (type=='d') btDeleteKey(mDB->idx[i],key,recno);
            if (type=='a') btInsertKey(mDB->idx[i],key,recno);
        }
    }
}

int  mDBi_idxopen(mDB,no)
mDBF *mDB;
int  no;
{
    char  fn2[81];
    int   i,l;

    for (i=0; i<MDB_KEYMAX; i++) {
        if (!no || no==(i+1)) {
            if (mDB->key[i][0] && mDB->idx[i]==0) {
                sprintf(fn2,"%s.idx%d",mDB->fn,i+1);
                l=mDBi_keylength(mDB,i);
                if (l)  mDB->idx[i]=btOpen(fn2, l, 1);
            }
        }
    }
    if (no && mDB->idx[no-1]==0) return(0);
    return(1);
}

void mDBi_idxclose(mDB,no)
mDBF *mDB;
int no;
{
    int      i;

    for (i=0; i<MDB_KEYMAX; i++) {
        if (!no || no==(i+1)) {
            if (mDB->idx[i]) {
                btClose(mDB->idx[i]);
                mDB->idx[i]=0;
            }
        }
    }
}

void mDBi_writedata(mDB,clr,blank,data)
mDBF  *mDB;
int  clr;
char *blank,*data;
{
    mDBi_field2rec(mDB,clr,blank,data);
    mDBi_rec2fields(mDB);
    memset(&mDB->record[1],32,mDB->recsize-3);
    strncpy(&mDB->record[1],mDB->varrec,strlen(mDB->varrec));
    fwrite(mDB->record,mDB->recsize,1,mDB->fp);
    mDBi_field2rec(mDB,clr,blank,mDB->varrec);
    mDB->updtime=time(0);
}

void mDBi_log(mDB,typ)
mDBF  *mDB;
char typ;
{
    if (mDB->logging && mDB->logfp) {
        time_t sec;
        struct tm *t;
        sec=time(0);
        t=localtime(&sec);
        fprintf(mDB->logfp,"%04d%02d%02d %02d%02d%02d %c|%s\n",
                1900+t->tm_year,t->tm_mon+1,t->tm_mday,
                t->tm_hour,t->tm_min,t->tm_sec,
                typ,mDB->varrec);
        fflush(mDB->logfp);
    }
}

int  mDBi_addrec(mDB,data,dst)
mDBF  *mDB;
char *data,*dst;
{
    UL	recno;

    if (mDB->delno) {
        recno=mDB->datastart+(mDB->dellist*mDB->recsize);
        fseek(mDB->fp,recno,SEEK_SET);
        if (fread(mDB->record,mDB->recsize,1,mDB->fp)!=0) {    /* read deleted record */
            mDB->delno--;
            mDB->dellist=atol(&mDB->record[1]);
            fseek(mDB->fp,recno,SEEK_SET);
        } else {                       /* should not happen, dellist is corrupted ?  */
            mDB->delno=0;		/* clean dellist and append */
            fseek(mDB->fp,0,SEEK_END);
            recno=ftell(mDB->fp);
        }
    } else {				/* no deleted records to fill, append it */
        fseek(mDB->fp,0,SEEK_END);
        recno=ftell(mDB->fp);
    }
    recno=(recno - mDB->datastart) / mDB->recsize;
    mDBi_writedata(mDB,1,0,data);
    mDBi_log(mDB,'a');
    mDBi_updkey(mDB,'a',recno);
    mDB->records++;
    mDB->recno=recno+1; 		/* recno from 1 - ... */
    if (dst)
        sprintf(dst,"%lu|%s",mDB->recno,mDB->varrec);
    else if (mDB_output)
        mDB_output(mDB,mDB->varrec);
    return(1);
}

/* return no of rows added */
int  mDB_add(fn,data,dst)
char *fn,*data,*dst;
{
    mDBF  *mDB;
    int no;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    no=mDBi_addrec(mDB,data,dst);
    return(no);
}

int  mDBi_read(mDB, recno, sw, dst)
mDBF  *mDB;
UL   recno;
int  sw;
char *dst;
{
    mDB->recno=recno;
    recno=mDB->datastart + (recno * mDB->recsize);
    if (fseek(mDB->fp,recno,SEEK_SET)!=0)  	   return(0);
    if (fread(mDB->record,mDB->recsize,1,mDB->fp)==0)   return(0);
    mDB->record[mDB->recsize-1]=0;
    strtrim(mDB->record);
    if (mDB->record[0]!='R')  return(0);
    strcpy(mDB->varrec,&mDB->record[1]);
    mDBi_field2rec(mDB,1,0,mDB->varrec);
    mDB->recno++;		                          /* recno from 1 - ... */
    if (dst)
        sprintf(dst,"%lu|%s",mDB->recno,mDB->varrec);
    else if (sw && mDB_output) {		     	       /* show record ? */
        mDB_output(mDB,mDB->varrec);
    }
    return(1);
}

int  mDB_first(fn,idxno,norows,dst)
char *fn, *dst;
int  idxno,norows;
{
    mDBF  *mDB;
    int   st;
    char  key[81];
    UL    recno;

    if ((mDB=mDBi_open(fn,0))==0)     return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    st=btFindFirstKey(mDB->idx[idxno],key,&recno);
    if (st==bOk) {
        do {
            mDBi_read(mDB,recno,1,dst);
        } while (--norows>0 && btFindNextKey(mDB->idx[idxno],key,&recno)==bOk);
        return(1);
    } else {
        return(0);
    }
}

int  mDB_last(fn,idxno,norows,dst)
char *fn,*dst;
int  idxno,norows;
{
    mDBF *mDB;
    int   st;
    char  key[81];
    UL    recno;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    st=btFindLastKey(mDB->idx[idxno],key,&recno);
    if (st==bOk) {
        do {
            mDBi_read(mDB,recno,1,dst);
        } while (--norows>0 && btFindPrevKey(mDB->idx[idxno],key,&recno)==bOk);
        return(1);
    } else {
        return(0);
    }
}

void mDBi_inp2key(mDB,idxno,inp,key)
mDBF  *mDB;
int  idxno;
char *inp, *key;
{
    char  keystr[81];
    int   i,fldno;

    strcpy(key,"");
    for (i=0; i<8; i++) {
        fldno=mDB->key[idxno][i];
        if (fldno==0) break;
        fldno--;

        getfield(i+1,inp,keystr,80,'|');
        mDBi_fieldfmt(0,mDB->fldtype[fldno],keystr,key);
    }
}

int   mDBi_equal(mDB, idxno, key, rkey, recno)
mDBF   *mDB;
int   idxno;
char  *key;
char  *rkey;
UL    *recno;
{
    mDBi_idxopen(mDB,idxno+1);
    mDBi_inp2key(mDB,idxno,key,rkey);
    return btSearchKey(mDB->idx[idxno],rkey,recno);
}

int  mDB_prev(fn, idxno, key, norows, dst)
int  idxno,norows;
char *fn,*key,*dst;
{
    mDBF  *mDB;
    char  rkey[81];
    UL    recno;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    if (mDBi_equal(mDB,idxno,key,rkey,&recno)==0) {
        while (norows-- && btFindPrevKey(mDB->idx[idxno],rkey,&recno)==0)
            mDBi_read(mDB,recno,1,dst);
        return(1);
    } else
        return(0);
}

int  mDB_find(fn, idxno, key, dst)
int  idxno;
char *fn,*key,*dst;
{
    mDBF  *mDB;
    char  rkey[81];
    UL    recno;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    if (mDBi_equal(mDB,idxno,key,rkey,&recno)==bOk) {
        mDBi_read(mDB,recno,1,dst);
        return(1);
    } else
        return(0);
}

int  mDB_next(fn,idxno,key,norows,dst)
int  idxno,norows;
char *fn,*key,*dst;
{
    mDBF  *mDB;
    char  rkey[81];
    UL    recno;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    if (mDBi_equal(mDB,idxno,key,rkey,&recno)==bOk) {
        while (norows-- && btFindNextKey(mDB->idx[idxno],rkey,&recno)==bOk)
            mDBi_read(mDB,recno,1,dst);
        return(1);
    } else
        return(0);
}

int  mDB_search(fn,idxno,key,norows,dst)
char *fn,*key,*dst;
int  idxno,norows;
{
    mDBF  *mDB;
    char  rkey[81];
    UL    recno;
    int   st,dir;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    dir=(norows<0)?(-1):(1);
    if (mDBi_idxopen(mDB,idxno+1)==0) return(0);
    mDBi_inp2key(mDB,idxno,key,rkey);
    if (*rkey) {
        st=btSearchKey(mDB->idx[idxno],rkey,&recno);
        if (dir!=1 && st==bErrEndOfIndex)
            st=btFindLastKey(mDB->idx[idxno],rkey,&recno);
        if (st!=bOk) {
            if (dir!=1)
                st=btFindPrevKey(mDB->idx[idxno],rkey,&recno);
            else
                st=bOk;   /* not exact match, but greater */
        }
    } else {
        if (dir==1)
            st=btFindFirstKey(mDB->idx[idxno],rkey,&recno);
        else
            st=btFindLastKey(mDB->idx[idxno],rkey,&recno);
    }
    if (st==bOk) {
        do {
            if (!mDBi_read(mDB,recno,1,dst))  break;
            if (dir==1)
                st=btFindNextKey(mDB->idx[idxno],rkey,&recno);
            else
                st=btFindPrevKey(mDB->idx[idxno],rkey,&recno);
            norows-=dir;
        } while (norows!=0 && st==0);
        return(1);
    } else
        return(0);
}

int mDB_delete(fn,idxno,key)
int  idxno;
char *fn,*key;
{
    mDBF  *mDB;
    int   ndel;
    UL	recno;
    char  rkey[81];
    UL    rrecno;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    ndel=0;
    mDBi_idxopen(mDB,0);
    if (idxno) idxno--;
    if (mDBi_equal(mDB,idxno,key,rkey,&rrecno)==bOk) {
        if (mDBi_read(mDB,rrecno,0,0)) {
            mDBi_log(mDB,'d');
            mDBi_updkey(mDB,'d',rrecno);
            recno=mDB->dellist;
            mDB->dellist=rrecno;
            mDB->delno++;
            memset(mDB->record,32,mDB->recsize-1);
            mDB->record[0]='D';
            sprintf(&mDB->record[1],"%lu|",recno);

            recno=mDB->datastart+(rrecno * mDB->recsize);
            if (fseek(mDB->fp,recno,SEEK_SET)!=0)  return(0);
            fwrite(mDB->record,mDB->recsize,1,mDB->fp);
            ndel++;
        }
    }
    if (ndel) mDBi_writehdr(mDB);
    return(ndel);
}

int mDB_update(fn,idxno,key,blank,data,dst)
int  idxno;
char *fn,*key,*blank,*data,*dst;
{
    mDBF  *mDB;
    int   nupd;
    UL	recno;
    char  rkey[81];
    UL    rrecno;

    nupd=0;
    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (mDBi_idxopen(mDB,idxno)==0) return(0);
    if (idxno) idxno--;
    if (mDBi_equal(mDB,idxno,key,rkey,&rrecno)==bOk) {
        mDBi_read(mDB,rrecno,0,dst);
        mDBi_log(mDB,'o');
        recno=mDB->datastart+(rrecno * mDB->recsize);
        if (fseek(mDB->fp,recno,SEEK_SET)!=0)  return(0);
        mDBi_updkey(mDB,'d',rrecno);
        mDBi_writedata(mDB,0,blank,data);
        mDBi_log(mDB,'u');
        mDBi_updkey(mDB,'a',rrecno);
        mDB->recno++;
        if (dst)
            sprintf(dst,"%lu|%s",mDB->recno,mDB->varrec);
        else if (mDB_output)
            mDB_output(mDB,mDB->varrec);
        nupd++;
    }
    if (nupd)    fflush(mDB->fp);
    return(1);
}

int  mDB_read(fn,recno,dst)
char *fn,*dst;
UL   recno;
{
    mDBF  *mDB;
    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (recno>0)
        recno--;
    else
        return(0);
    return mDBi_read(mDB,recno,1,dst);
}

int  mDB_dropindex(fn,idxno)
char  *fn;
int   idxno;
{
    mDBF  *mDB;
    char  fn2[81];
    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (idxno) idxno--;
    if (mDB->key[idxno][0]) {
        mDBi_idxclose(mDB,idxno+1);			/* close index    */
        mDB->keyno--;
        mDB->key[idxno][0]=0;
        mDBi_writehdr(mDB);
        sprintf(fn2,"%s.idx%d",mDB->fn,idxno+1);
        unlink(fn2);
        return 1;
    }
    return(0);
}

int  mDB_rebuildindex(fn,idxno)
char *fn;
int  idxno;
{
    mDBF  *mDB;
    char  fn2[81];
    int   i,l,ok;
    UL    recno;
    char  rkey[81];

    if ((mDB=mDBi_open(fn,1))==0) return(0);  /* diff versions allowed */
    ok=0;
    for (i=0; i<MDB_KEYMAX; i++) {
        if (!idxno || idxno==(i+1)) {
            if (mDB->key[i][0]!=0 ) {
                sprintf(fn2,"%s.idx%d",mDB->fn,i+1);
                l=mDBi_keylength(mDB,i);
                /* --------------- make index ------------------ */
                if (l==0) return 0;
                unlink(fn2);
                mDB->idx[i]=btOpen(fn2, l, 1);
                /* --------- read data and build index ----------*/
                sprintf(fn2,"Building index %d",i+1);
                mDB_output(0,fn2);
                if (fseek(mDB->fp,mDB->datastart,SEEK_SET)!=0)  	   return(0);
                recno=0;
                while (fread(mDB->record,mDB->recsize,1,mDB->fp)!=0) {
                    if (mDB->record[0]!='D') {
                        mDB->record[mDB->recsize-1]=0;
                        strtrim(mDB->record);
                        strcpy(mDB->varrec,&mDB->record[1]);
                        mDBi_field2rec(mDB,1,0,mDB->varrec);
                        mDBi_buildkey(mDB,i,rkey);
                        btInsertKey(mDB->idx[i],rkey,recno);
                    }
                    recno++;
                }
                mDBi_idxclose(mDB,i+1);
                ok++;
            }
        }
    }
    if (ok) {
        if (!idxno)  strcpy(mDB->ID,mDBVERSION);
        mDBi_writehdr(mDB);
        return(1);
    }
    return(0);
}

int  mDB_createindex(fn,idxno,key)
int  idxno;
char *fn,*key;
{
    mDBF  *mDB;
    char  *p,*s;
    int   i;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (idxno) idxno--;
    if (mDB->key[idxno][0]==0) {			/* if empty */
        /* ----------- determine index fields ---------- */
        i=0;
        s=p=&key[0];
        while (s) {
            mDB->key[idxno][i++]=atoi(p);
            s=strchr(p,':');
            if (s) p=&s[1];
        }
        mDB->keyno++;
        if (mDB_rebuildindex(fn,idxno+1)==0)
            mDB->keyno--;
        else
            return(1);
    }
    return(0);	  /* no success */
}

int mDB_kill() {
    while (mDBFS) {
        mDBFS->no=1;
        mDBi_close(mDBFS);
    }
    return(1);
}


int mDB_log(fn,key)
char *fn,*key;
{
    mDBF  *mDB;

    if ((mDB=mDBi_open(fn,0))==0) return(0);
    if (strcmp(key,"on")==0) {
        if (mDB && !mDB->logging) {
            char fn2[81];
            sprintf(fn2,"%s.log",mDB->fn);
            mDB->logfp=fopen(fn2,"a");
            mDB->logging=1;
            mDBi_writehdr(mDB);
        }
    } else if (mDB->logging) {
        fclose(mDB->logfp);
        mDB->logging=0;
        mDBi_writehdr(mDB);
    }
    return(1);
}

int mDB_init() {
    /* no idea what to do here yet, perhaps read a config file */
    return(1);
}

void mDB_flushall() {
    mDBF  *mDB;

    for (mDB=mDBFS; mDB!=0; mDB=mDB->next) {
        mDBi_idxclose(mDB,0);		/* close all open indexes      */
        if (mDB->logging && mDB->logfp) fflush(mDB->logfp);
        fflush(mDB->fp);
    }
}

void mDB_exit() {
    mDB_kill();
}
