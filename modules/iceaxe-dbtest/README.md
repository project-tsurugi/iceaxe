# iceaxe-dbtest

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * iceaxe-core
  * Tsubakuro

## How to execute

### Execute

```bash
cd modules/iceaxe-dbtest
../../gradlew dbtest
```

### Execute with iceaxe-core, Tsubakuro that installed locally

Execute with Gradle Property `mavenLocal` .

```bash
cd modules/iceaxe-dbtest
../../gradlew dbtest -PmavenLocal
```

### Execute with endpoint

```bash
cd modules/iceaxe-dbtest
../../gradlew dbtest -Pdbtest.endpoint=tcp://localhost:12345
```

### Execute with credential

```bash
cd modules/iceaxe-dbtest
../../gradlew dbtest \
-Pdbtest.user=user \
-Pdbtest.password=password \
-Pdbtest.auth-token=token \
-Pdbtest.credentials=/path/to/credential-file
```

For tests other than credential, specifying only one of `user`, `auth-token`, or `credentials` is sufficient. If none of these are specified, authentication will be performed using the user `tsurugi`.

In the credential test, anything not specified is skipped.

### Execute with lob path mapping

Lob path mapping for privileged mode is specified using the format `<client-path>:<server-path>`.

#### example for MS-Windows command prompt

```
docker container run -d -p 12345:12345 -p 52345:52345 -v C:/tmp/client:/mnt/client -v C:/tmp/tsurugi:/opt/tsurugi/var/data/log --name tsurugi -e GLOG_v=30 ghcr.io/project-tsurugi/tsurugidb:latest

cd modules\iceaxe-dbtest
..\..\gradlew dbtest ^
-Pdbtest.lob-send-path-mapping=C:/tmp/client:/mnt/client ^
-Pdbtest.lob-recv-path-mapping=C:/tmp/tsurugi:/opt/tsurugi/var/data/log
```

### Execute with blob relay service endpoint

#### example

```bash
cd modules/iceaxe-dbtest
../../gradlew dbtest \
-Pdbtest.blob-relay-service-endpoint=dns:///localhost:52345
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)

