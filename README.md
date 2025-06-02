# Iceaxe - java library for Tsurugi

Iceaxe is a Java library that executes SQL on Tsurugi database.

Iceaxe is not JDBC, but the program layer is similar to JDBC.

[The book about Tsurugi](https://info.nikkeibp.co.jp/media/LIN/atcl/books/091300039/) explains Iceaxe.

## Requirements

* Java `>= 11`

* dependent modules:
  * [Tsubakuro](https://github.com/project-tsurugi/tsubakuro)
  * SLF4J

## Javadoc

[![javadoc](https://javadoc.io/badge2/com.tsurugidb.iceaxe/iceaxe-core/javadoc.svg)](https://javadoc.io/doc/com.tsurugidb.iceaxe/iceaxe-core)

* GitHub Pages (Latest version)
  * https://project-tsurugi.github.io/iceaxe/
* javadoc.io (All versions of javadoc can be referenced)
  * https://javadoc.io/doc/com.tsurugidb.iceaxe/iceaxe-core/latest/index.html

## How to use

Iceaxe is hosted on Maven Central Repository.

* https://central.sonatype.com/artifact/com.tsurugidb.iceaxe/iceaxe-core/overview

To use on Gradle, add Iceaxe library to dependencies.

```
dependencies {
    implementation 'com.tsurugidb.iceaxe:iceaxe-core:1.9.0'

    implementation 'org.slf4j:slf4j-simple:1.7.32'
}
```

## Example

```java
import java.net.URI;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var endpoint = URI.create("tcp://localhost:12345");
var connector = TsurugiConnector.of(endpoint);
try (var session = connector.createSession()) {
    try (var ps = session.createStatement("update customer set c_age = c_age + 1")) {
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        var tm = session.createTransactionManager(setting);
        tm.execute(transaction -> {
            transaction.executeAndGetCount(ps);
        });
    }
}
```

See also [iceaxe-examples](modules/iceaxe-examples/src/main/java/com/tsurugidb/iceaxe/example).

## How to build

```bash
cd iceaxe
./gradlew build
```

### Build with Tsubakuro that installed locally

First, check out and install Tsubakuro locally, and build Iceaxe with Gradle Property `mavenLocal` .

```bash
cd tsubakuro
./gradlew PublishToMavenLocal -PskipBuildNative

cd iceaxe
./gradlew build -PmavenLocal
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)