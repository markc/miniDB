/* -------------------------------------------------------------------------- *
 * Description : functions to extract fields from a string
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
#include "mdb_func.h"

int	FLD_strip_lead  = 1;		/* strip leading  spaces ? */
int	FLD_strip_trail = 1;		/* strip trailing spaces ? */

/* strtrim:
 * strip trailing spaces,tabs,CR,LF, ... from a string
 */
void	strtrim(char *str) {
    int i;
    for (i=strlen(str)-1; (i>=0 && str[i]>0 && str[i]<=32); i--) str[i]=0;
}


/* nofield:
 * count no fields in string using a seperator
 * returns: 1...n  nofields
 */
int	nofield(char *str, char sep) {
    int	no;
    char	*ep;

    no=0;
    for (ep=str; *ep!=0; ep++)  if (*ep==sep) no++;
    if (str!=ep) no++;
    return (no);
}

/* getfield:
 * extracts a string field from a string with a fieldseperator
 * with a max length ('|')
 * returns: 0 if not found else returns fieldno extracted
 */
int	getfield(int fldno, char *str, char *dst, int max, char sep) {
    int	no;
    char	*sp, *ep;

    no=0;
    sp=ep=str;
    while (1) {
        if (*ep==sep || *ep==0) {
            no++;
            if (fldno==no || *ep==0) break;
            sp=ep+1;
        } else if (ep==sp && *ep==' ' && FLD_strip_lead) sp++;		/*002*/
        ep++;
    }
    if (fldno==no) {
        for (no=0; (sp!=ep && no<max); no++) {				/*002*/
            dst[no]=*sp;
            sp++;
        }
        dst[no]='\0';
        if (FLD_strip_trail) strtrim(dst);				/*002*/
        return(no);
    } else
        dst[0]='\0';
    return(0);
}

/* getfldint:
 * extract a integer field from a string with a fieldseperator
 * returns: 0 if not found else returns fieldno extracted
 */
int	getfldint(int fldno, char *str, int *dst, char sep) {
    char tmp[21];
    int  ret;

    *dst=0;
    ret=getfield(fldno, str, tmp, 20, sep);
    if (ret) *dst=atoi(tmp);

    return(ret);
}

/* getfldlong:
 * extract a long field from a string with a fieldseperator
 * returns: 0 if not found else returns fieldno extracted
 */
int	getfldlong(int fldno, char *str, long *dst, char sep) {
    char tmp[21];
    int  ret;

    *dst=0L;
    ret=getfield(fldno, str, tmp, 20, sep);
    if (ret) *dst=atol(tmp);

    return(ret);
}

/* getflddbl:
 * extract a double field from a string with a fieldseperator
 * returns: 0 if not found else returns fieldno extracted
 */
int	getflddbl(int fldno, char *str, double *dst, char sep) {
    char tmp[21];
    int  ret;

    *dst=0;
    ret=getfield(fldno, str, tmp, 20, sep);
    if (ret) *dst=atof(tmp);

    return(ret);
}

/* fndfield:
 * search for start of field in string and optional clears its contents
 * returns: -1 if not found, else returns the offset (0-based)
 */
int	fndfield(int fldno, char *str, int clr, char sep) {
    int	no,ofs;
    char	*sp, *ep;

    ofs=no=0;
    sp=ep=str;
    while (1) {
        if (*ep==sep || *ep==0) {
            no++;
            if (fldno==no || *ep==0) break;
            sp=ep+1;
        } else if (ep==sp && *ep==' ' && FLD_strip_lead) sp++;
        ep++;
        ofs++;
    }
    if (fldno==no) {
        if (clr) while (sp!=ep) *sp++=' ';
        return(ofs);			/* return start of field 0-n */
    } else
        return(-1);
}
