# SQLite Storage Manager

## Description

This project is a new disck-oriented storage manager for the [SQLite](<https://www.sqlite.org/index.html>) DBMS. Such a storage manager assumes that the primary storage location of the database is on disk. We can use this storage manager in SQLite based on SQLite's [Virtual Table](<https://www.sqlite.org/vtab.html>) interface, without changing application-level code. 

## Function

- **Extendable Hash Table** : The hash table uses unordered buckets to store unique key/value pairs. It supports the ability to insert/delete key/value entries without specifying the max size of the table. It can automatically grow in size as needed. Use Google CityHash as hash function.
- **Buffer Pool Manager** : The buffer pool manager is responsible for moving physical pages back and forth from main memory to disk. It allows a DBMS to support databases that are larger than the amount of memory that is available to the system. The manager uses LRU page replacement policy.

## Use Google CityHash

```
git clone git@github.com:google/cityhash.git
cd cityhash
./configure
make all CXXFLAGS=-fPIC
sudo make install
```

Add `link_libraries(/usr/local/lib/libcityhash.a)` to CMakeLists.txt.

**check the libcityhash.a is position-independent** : 

```
sudo cd /usr/local/lib
sudo ar -x libcityhash.a
readelf --relocs city.o | egrep '(GOT|PLT|JU?MP_SLOT)'
```

If there is any output, the libcityhash.a is position-independent.

## SQLite Project Source Code

### Build
```
mkdir build
cd build
cmake ..
make
```
Debug mode:

```
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

### Testing
```
cd build
make check
```

### Run virtual table extension in SQLite
Start SQLite with:
```
cd build
./bin/sqlite3
```

In SQLite, load virtual table extension with:

```
.load ./lib/libvtable.dylib
```
or load `libvtable.so` (Linux), `libvtable.dll` (Windows)

Create virtual table:  
1.The first input parameter defines the virtual table schema. Please follow the format of (column_name [space] column_type) seperated by comma. We only support basic data types including INTEGER, BIGINT, SMALLINT, BOOLEAN, DECIMAL and VARCHAR.  
2.The second parameter define the index schema. Please follow the format of (index_name [space] indexed_column_names) seperated by comma.
```
sqlite> CREATE VIRTUAL TABLE foo USING vtable('a int, b varchar(13)','foo_pk a')
```

After creating virtual table:  
Type in any sql statements as you want.
```
sqlite> INSERT INTO foo values(1,'hello');
sqlite> SELECT * FROM foo ORDER BY a;
a           b         
----------  ----------
1           hello   
```
See [Run-Time Loadable Extensions](https://sqlite.org/loadext.html) and [CREATE VIRTUAL TABLE](https://sqlite.org/lang_createvtab.html) for further information.

### Virtual table API
https://sqlite.org/vtab.html

### TODO
* update: when size exceed that page, table heap returns false and delete/insert tuple (rid will change and need to delete/insert from index)
* delete empty page from table heap when delete tuple
* implement delete table, with empty page bitmap in disk manager (how to persistent?)
* index: unique/dup key, variable key
