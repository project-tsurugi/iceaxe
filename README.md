# Iceaxe - Java High-level-API

Iceaxe is a Java library that executes SQL on Tsurugi database.

## Requirements

* Java `>= 11`

* access to installed dependent modules:
  * Tsubakuro

## Javadoc

* https://project-tsurugi.github.io/iceaxe/

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
    implementation 'com.tsurugidb.iceaxe:iceaxe-core:1.0.1'
}
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)