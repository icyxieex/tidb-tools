## syncer

syncer is a tool for syncing source database data to target database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to build

```
make deps && sh deps.sh (optional, install golang dependent packages)
make build
```

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

// batch commit count
batch = 1

// from MySQL config
[from]
host = "127.0.0.1"
user = "root"
password = ""
port = 3306
name = "test"

// to TiDB config
[to]
host = "127.0.0.1"
user = "root"
password = ""
port = 4000
name = "test"
```

## Example

```
./syncer -config=config.toml
```

## License
Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
