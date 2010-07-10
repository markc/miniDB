/* -------------------------------------------------------------------------- *
 * Description : field extraction from string - definitions
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
#ifndef GETFIELD_H
#define GETFIELD_H

extern	int	FLD_strip_lead;
extern	int	FLD_strip_trail;

void	strtrim   (char *str);

int	nofield   (char *str, char sep);
int fndfield  (int fldno, char *str, int     clr, char sep);

int	getfield  (int fldno, char *str, char   *dst, int  max, char sep);
int	getfldint (int fldno, char *str, int    *dst, char sep);
int	getfldlong(int fldno, char *str, long   *dst, char sep);
int	getflddbl (int fldno, char *str, double *dst, char sep);

#endif
