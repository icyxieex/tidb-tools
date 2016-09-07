## syncer

syncer is a tool for syncing source database data to target database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to use

```
Usage of syncer:
  -L string
        log level: debug, info, warn, error, fatal (default "info")
  -b int
        batch commit count (default 1)
  -config string
        Config file
  -meta string
        syncer meta info (default "syncer.meta")
  -pprof-addr string
        pprof addr (default ":10081")
  -server-id int
        MySQL slave server ID (default 101)
```

## Config
```
// log level info
log-level = "info"

// server id, used for register slave
server-id = 101

// meta for binlog savepoint
meta = "syncer.meta"

// parallel db worker count
worker-count = 1

// batch commit count
batch = 1

// pprof addr
pprof-addr = ":10081"

// from MySQL config
[from]
host = "127.0.0.1"
user = "root"
password = ""
port = 3306

// to TiDB config
[to]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000
```

## Example

```
./bin/syncer -config=syncer/config.toml
```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
