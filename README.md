# ComboTree

## Request

**CPU flag**:

- *clflush*: clflush OR clflushopt OR clwb
- *streaming store*: sse2 OR avx2 OR avx512vl
- *bit instruction*: bmi1 AND bmi2

**endian**: little endian

**C++ standard**: C++11

**Dependence**
***PGM-Index***
[https:] https://github.com/gvinciguerra/PGM-index.git
    
***Xindex***
[https:] https://ipads.se.sjtu.edu.cn:1312/opensource/xindex.git
<!-- [MKL]:
添加intel源：
yum -y install yum-utils
yum-config-manager --add-repo https://yum.repos.intel.com/mkl/setup/intel-mkl.repo
下载并安装MKL：
yum install -y intel-mkl -->
<!-- 内存检测 -->
<!-- sudo valgrind --leak-check=full --show-reachable=yes --trace-children=yes -s -->
**YCSB**
./ycsb -db letree -threads 1 -P ../include/ycsb/workloads/
./ycsb -db combotree -threads 1 -P ../include/ycsb/insert_ratio/

**OSM**
osmconvert: wget -O - http://m.m.i24.cc/osmconvert.c | cc -x c - -lz -O3 -o osmconvert
pbftoosm: osmconvert region.pbf -o=region.osm
statistics: osmconvert germany.osm.pbf --out-statistics
osmtocsv: osmconvert shops.osm --all-to-nodes --csv="@id @lon @lat amenity shop name" --csv-headline
