#!/bin/bash
#
[ -f "data/demo.*" ] && rm data/demo.*
#
echo "creating a demo file"
./mdb demo c "id:i8|seak:c8|name:c30|street:a30|postal:c8|place:a30|_key:1|"
#
echo "building demo set..."
cat data/wordlist |awk '
BEGIN {
  i=1000;
}
{
  sk=substr($1,1,8);
  pl=substr($1,2);
  i=i+1;
  printf("%d|%s|%s|street %d|ZIP %d|%s|\n",i,sk,$1,i,i,pl); 
}' >data/demo.asc
no=`wc -l data/demo.asc`
#
# appending data from stdin ("-")
date
echo "Adding $no records.."
./mdb demo a "-" <data/demo.asc >/dev/null
#
date
echo "Adding index 2 (seak/id).."
./mdb -k2 demo C "2:1"
#
date
echo "Adding index 3 (postal/id).."
./mdb -k3 demo C "5:1"
date
echo "done."
#
echo "Try some of the following:"
echo "./mdb demo i"
echo "./mdb -r10 demo f"
echo "./mdb -r10 demo s 1401"
echo "./mdb -k2 -r10  demo s database "
echo "./mdb -k2 -r-10 demo s open"
echo "./mdb -k1 demo e '22991'"
