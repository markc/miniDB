/* ------------------------------------------------------------------------- *
 * Description : miniDB to JSON output (suggestion from Mark Constable)
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
#include <string.h>
#include <stdlib.h>
#include <getopt.h>

#include "mdb_func.h"

char fld[1025];
char dat[1025];
char buf[1025];
int  nofld=0;
int  norec=0;
int  swrow=0;

syntax() {
    puts("Syntax  : mdb2json [-p <prefix>] [-r]");
    puts("          take mdb output and make json style format from it");
    puts("Options : -p  field prefixid");
    puts("          -r  each field on seperate line ");
    puts("       ex. mdb -f -r3 demo s \"1244\" |mdb2json -p \"tbl_\" ");
    puts("");
    exit(1);
}

void  formatval(char *str,char *dst) {
    int n;
    char *ptr;

    n=0;
    for (ptr=str; (n==0 && *ptr!=0); ptr++)  if (strchr("0123456789.",*ptr)==0) n=1;
    if (n==0 && *str!=0)
        strcpy(dst,str);
    else 
        sprintf(dst,"\"%s\"",str);
}

void jsondata(char *prefix) {
    int   i,mx;
    char  nam[133],val[133],val2[133];

    mx=nofield(dat,'|');
    if (mx<nofld) {
        dat[0]=0;
        norec=0;
    } else 
        norec++;
    if (!swrow)  printf("\t");
    for (i=1; i<nofld; i++) {
        getfield(i,fld,nam,132,'|');
        getfield(i,dat,val,132,'|');
        if (nam[0]) {
            formatval(val,val2);
            printf("%s \"%s%s\": %s",(swrow)?"\t":"",prefix,nam,val2);
        }
        if ((i+1)<nofld) {
           printf(",");
           if (swrow) printf("\n");
        } else
           printf("\n");
    }
}

int  main(argc, argv)
int  argc;
char *argv[];
{
    int  i,ch;
    char *n, prefix[81];

    swrow=0;
    prefix[0]=0;
    while ((ch=getopt(argc,argv,"p:r"))!=-1) {
        switch (ch) {
        case 'p' :
            strcpy(prefix,optarg);
            break;
        case 'r' :
            swrow=1;
            break;
        default  :
            syntax();
            break;
        }
    }
    argc -=optind;
    argv +=optind;
    if (argc>0)  syntax();

    n=fgets(fld,1023,stdin);		  /* read fieldnames */
    if (n && strchr(fld,'|')!=0) {
        nofld=nofield(fld,'|');
        i=0;
        printf("{ \"%sResult\": [\n",prefix);
        while (fgets(buf,1023,stdin)!=0) {   /* read record */
            if (i) puts(",{ ");
            else printf("     { \n");
            strcpy(dat,buf);
            jsondata(prefix);
            printf("     }");
            i++;
        }
        printf("\n     ],\n");
        printf("  \"%sNoRows\": %d \n",prefix,norec);
        puts("}");
    }
    exit(0);
}

