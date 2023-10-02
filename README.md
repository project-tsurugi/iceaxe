# Iceaxe - Java High-level-API

Iceaxe is a Java library that executes SQL on Tsurugi database.

[![javadoc](https://javadoc.io/badge2/com.tsurugidb.iceaxe/iceaxe-core/javadoc.svg)](https://javadoc.io/doc/com.tsurugidb.iceaxe/iceaxe-core)

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * Tsubakuro

## How to build

```
cd iceaxe
./gradlew build
```

### Build with Tsubakuro that installed locally

First, check out and install Tsubakuro locally, and build Iceaxe with Gradle Property `mavenLocal` .

```bash
cd tsubakuro
./gradlew publishMavenJavaPublicationToMavenLocal

cd iceaxe
./gradlew build -PmavenLocal
```

## How to use

To use on Gradle, add Iceaxe library to dependencies.

```
dependencies {
    implementation 'com.tsurugidb.iceaxe:iceaxe-core:1.0.0-SNAPSHOT'
}
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)