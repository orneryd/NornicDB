#!/bin/bash
# cmp.sh base.txt head.txt: median ns/op, B/op, allocs/op per benchmark, head vs base.
awk '
FNR==1{side=(side==""?"base":"head")}
/^Benchmark/{ name=$1; sub(/-[0-9]+$/,"",name); for(i=3;i<=NF;i++){ if($(i+1)=="ns/op") v["ns",side,name]=v["ns",side,name] " " $i; if($(i+1)=="B/op") v["B",side,name]=v["B",side,name] " " $i; if($(i+1)=="allocs/op") v["al",side,name]=v["al",side,name] " " $i } names[name]=1 }
function med(s,  a,n,i,j,t){n=split(s,a," "); for(i=1;i<=n;i++)for(j=i+1;j<=n;j++)if(a[j]+0<a[i]+0){t=a[i];a[i]=a[j];a[j]=t}; if(n==0)return -1; return (n%2)?a[(n+1)/2]:(a[n/2]+a[n/2+1])/2}
END{ for(n in names){ b=med(v["ns","base",n]); h=med(v["ns","head",n]); if(b<=0||h<0)continue; bb=med(v["B","base",n]); hb=med(v["B","head",n]); ba=med(v["al","base",n]); ha=med(v["al","head",n])
    printf "%-70s %11.0f %11.0f %+7.1f%% | B %9.0f %9.0f %+6.1f%% | al %7.0f %7.0f\n", substr(n,11,70), b, h, 100*(h-b)/b, bb, hb, (bb>0?100*(hb-bb)/bb:0), ba, ha } }' "$1" "$2" | sort
