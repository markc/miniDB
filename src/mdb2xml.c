/* ------------------------------------------------------------------------- *
 * Description : miniDB to xml output
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

char title[1025];
char fld[1025];
char dat[1025];
char buf[1025];

syntax() {
    puts("Syntax  : mdb2xml [-r] [-t <titlefields>");
    puts("          take mdb output and make xml style format from it");
    puts("Options : -p  field prefixid");
    puts("          -r  force row type output");
    puts("          -t  output titlefields");
    puts("       ex. mdb -f -r3 demo s \"1244\" |mdb2xml -p \"tbl_\" ");
    puts("");

    puts("");
    exit(1);
}


void xmldata(char *prefix) {
    int   i,mx;
    char  nam[133],val[133];

    mx=nofield(fld,'|');
    for (i=1; i<=mx; i++) {
        getfield(i,fld,nam,132,'|');
        getfield(i,dat,val,132,'|');
        if (nam[0]) {
           printf("  <%s%s>%s</%s%s>\n",prefix,nam,val,prefix,nam);
        }
    }
}

int  main(argc, argv)
int  argc;
char *argv[];
{
    int  i,ch,swrow;
    char *n, prefix[81];

    swrow=0;
    prefix[0]=title[0]=0;
    while ((ch=getopt(argc,argv,"p:rt:"))!=-1) {
        switch (ch) {
        case 'p' :
            strcpy(prefix,optarg);
            break;
        case 'r' :
            swrow=1;
            break;
        case 't' :
            strcpy(title,optarg);
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
        if (title[0]) {
            puts("<title>");
            strcpy(dat,title);
            xmldata(prefix);
            puts("</title>");
        }
        i=0;
        while (fgets(buf,1023,stdin)!=0) {   /* read record */
            if (i) {
                puts("<row>");
                xmldata(prefix);
                puts("</row>");
            }
            strcpy(dat,buf);
            i++;
        }
        if (i==0 && !swrow) {		/* no data found */
            strcpy(dat,"");
            i++;
        }
        if (i) {
            if (swrow || i>1) puts("<row>");
            xmldata(prefix);
            if (swrow || i>1) puts("</row>");
        }
    }
    exit(0);
}

