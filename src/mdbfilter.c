/* -------------------------------------------------------------------------- *
 * Description : miniDB output filter
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
#include <stdlib.h>
#include <getopt.h>
#include "mdb_func.h"

syntax() {
    puts("Syntax: mdbfilter [-f] <fields>");
    puts("  Options  : -f  output fieldnames");
    puts("  Parameter:  select only the fields needed");
    puts("              ex. \"recno|cust_id|name_id|\" ");
    puts("");
    exit(1);

}

void do_error(char *s) {
    puts(s);
    exit(1);
}

int  main(argc, argv)
int  argc;
char *argv[];
{
    char buf[513],fldval[257],tmp[61];
    int  fld[100];
    int  mxfld,ch,header;
    int  i,j;

    header=0;
    while ((ch=getopt(argc,argv,"f"))!=-1) {
        switch (ch) {
        case 'f' :
            header=1;
            break;
        default  :
            syntax();
            break;
        }
    }
    argc -=optind;
    argv +=optind;

    if (argc<1)  syntax();

    mxfld=0;
    while (fgets(buf,512,stdin)!=0) {
        if (mxfld==0) {    		/* find out which fields to take */
            for (j=1; getfield(j,argv[0],fldval,60,'|')!=0; j++) {
                if (fldval[0]==0) break;
                for (i=1; getfield(i,buf,tmp,60,'|')!=0; i++)
                    if (strcmp(fldval,tmp)==0) {
                        if (header) printf("%s|",tmp);
                        fld[mxfld++]=i;
                        break;
                    }
            }
            if (header) printf("\n");
        } else {
            for (i=0; i<mxfld; i++) {
                getfield(fld[i],buf,fldval,256,'|');
                printf("%s|",fldval);
            }
            printf("\n");
        }
    } /*while*/
}
