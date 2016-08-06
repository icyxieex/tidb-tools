## importer

importer is a tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to build

```
make deps && sh deps.sh (optional, install golang dependent packages)
make build
```

## How to use

```
Usage of importer:
  -D string
    	set the database name (default "test")
  -L string
    	log level: debug, info, warn, error, fatal (default "info")
  -P int
    	set the database host port (default 3306)
  -b int
    	insert batch commit count (default 1)
  -c int
    	parallel worker count (default 1)
  -config string
    	Config file
  -h string
    	set the database host ip (default "127.0.0.1")
  -i string
    	create index sql
  -max-idle-conns int
    	set maximum number of connections (default 100)
  -max-open-conns int
    	set the maximum number of open connections  (default 100)
  -n int
    	total job count (default 1)
  -p string
    	set the database password
  -t string
    	create table sql
  -u string
    	set the database user (default "root")
```

## Example

```
./importer -t "create table t(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);" -i "create unique index u_b on t(b);" -c 1 -n 10 -P 4000
```

Also，we have some interesting features, like:

- range
```
./importer -t "create table t(a int comment '[[range=1,10]]');" -P 4000 -c 1 -n 10
```
Then the table value will be like this
```
mysql> select * from t;
+------+
| a    |
+------+
|    5 |
|    6 |
|    9 |
|    5 |
|    3 |
|    3 |
|   10 |
|    9 |
|    3 |
|   10 |
+------+
10 rows in set (0.00 sec)
```

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
