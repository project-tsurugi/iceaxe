# Iceaxe内部構造

Iceaxeの内部構造やその意図について説明する。

- 当ドキュメントで[iceaxe-usage.md](iceaxe-usage.md)の補足も行う。



## クラス名の命名ルール

Iceaxeではクラス名に接頭辞を付けている。

何も付けないと、Tsubkuroやその他のライブラリーとクラス名（単純名）が同一になってしまい、コーディングやソースコードリーディングの際に混乱を来す為。

| 接頭辞    | 説明                                                         | 備考                                         |
| --------- | ------------------------------------------------------------ | -------------------------------------------- |
| `Tsurugi` | 基本的にはこの接頭辞が付けられている。                       | 概ね、処理を行うのが主目的のクラス           |
| `Tg`      | アプリケーション開発者が直接使用する頻度が高いクラス（アプリケーション開発者が直接インスタンスを生成するクラスや列挙型等）については、クラス名を短くする為にこの接頭辞が付けられている。 | 概ね、値を保持するのが主目的のクラスや列挙型 |
| `Iceaxe`  | Iceaxe内部で使用する目的のクラスはこの接頭辞が付けられている。 |                                              |

### 略語

Javaでは、クラス名やメソッド名に使用する単語は省略しないことが推奨されている。

しかし多くの単語が連なると冗長な名前になってしまうので、Iceaxeでは以下のような略語を使っている。

| 単語                 | 略語 |
| -------------------- | ---- |
| `Tsurugi`            | `Tg` |
| `Transaction`        | `Tx` |
| `TransactionManager` | `Tm` |



## Tsubakuroとの対応

Iceaxeの主要なクラスは、Tsubakuroのクラスをラップ（内包）している。

| Iceaxeクラス                                           | Tsubakuroクラス                                            |
| ------------------------------------------------------ | ---------------------------------------------------------- |
| TsurugiConnector                                       | Connector                                                  |
| TsurugiSession                                         | Session・SqlClient                                         |
| TsurugiTransaction                                     | Transaction                                                |
| TgTxOption                                             | SqlRequest.TransactionType<br>SqlRequest.TransactionOption |
| TgCommitType                                           | SqlRequest.CommitStatus                                    |
| TsurugiSqlPreparedQuery<br>TsurugiSqlPreparedStatement | PreparedStatement                                          |
| TgParameterMapping                                     | SqlRequest.Placeholder<br>SqlRequest.Parameter             |
| TgBindVariable・TgBindVariables                        | SqlRequest.Placeholder                                     |
| TgBindParameter・TgBindParameters                      | SqlRequest.Parameter                                       |
| TsurugiQueryResult<br>TsurugiResultRecord              | ResultSet                                                  |
| TgDataType                                             | SqlCommon.AtomType                                         |

### API方式

Tsubakuroは非同期APIである。すなわち、Tsubakuroのメソッドを呼び出すとFuture（実体としてはTsubakuro独自の`FutureResponse`）が返ってくる。

一般に、非同期APIより同期APIの方がアプリケーション開発者にとって扱いやすいので、Iceaxeは同期APIになっている。

Iceaxeで「Tsubakuroの`FutureResponse`を返すメソッド」を呼び出すほとんどのクラスは、内部で`FutureResponse`を保持する。
必要になった時点で`FutureResponse`から値を取り出して使用する。（いわゆる遅延評価）

> **Note**
>
> `FutureResponse`が作られるのは、Tsubakuroが通信を行う為である。通信の結果（応答）が`FutureResponse`から取得できる。
>
> 遅延評価するようにすれば、Tsubakuroが通信を行っている間に、クライアント側で他の処理を実施できる。

### エンドポイント

どのTsurugi DBに接続するかという情報と接続方法を表すエンドポイントの内容は、Tsubakuroの仕様に準ずる。
現時点では以下の通り。

Tsurugi DBへの接続方法には、TCP接続とIPC接続がある。

- TCP接続
  - データ通信にTCP/IP（ソケット通信）を用いる方法。
  - リモートから接続可能。
  - 現時点のTCP接続は暫定実装という位置付けであり、性能はあまり良くない。
- IPC接続
  - データ通信にLinuxのプロセス間通信（共有メモリー）を用いる方法。
  - Tsurugi DBと同一マシン上でないと使用できない。
  - 現時点ではTCP接続よりも高速。

TCP接続の場合は、URIのスキーマ名をtcpとし、TCP接続エンドポイントのIPアドレス（ホスト名）とTCPポート番号を指定する。
例：`tcp://localhost:12345`

IPC接続の場合は、URIのスキーマ名をipcとし、IPC接続エンドポイントのデータベース名を指定する。
例：`ipc:tateyama`


これらのエンドポイントの値は、Tsurugi DBを起動する際に読み込まれる設定ファイルの `stream_endpoint` や `ipc_endpoint` に記述されている。

#### Tsurugi DBの設定ファイルの例

```
[stream_endpoint]
    port=12345

[ipc_endpoint]
    database_name=tateyama
```

### セッション数の上限

DBサーバーに接続するセッション数は、DBサーバー側に上限がある。

最大セッション数は、Tsurugi DBを起動する際に読み込まれる設定ファイルの `stream_endpoint` や `ipc_endpoint` に記述されている。

#### Tsurugi DBの設定ファイルの例

```
[stream_endpoint]
    threads=60

[ipc_endpoint]
    threads=60
```

### トランザクション種別

Iceaxeのトランザクション種別の名称は、Tsubakuroの`TransactionType`とは異なっている。

| Iceaxeトランザクション種別 | Tsubakuro `TransactionType` | 備考（`TransactionType`のJavadoc）                  |
| -------------------------- | --------------------------- | --------------------------------------------------- |
| OCC                        | `SHORT`                     | short transactions (optimistic concurrency control) |
| LTX                        | `LONG`                      | long transactions (pessimistic concurrency control) |
| RTX                        | `READ_ONLY`                 | read only transactions                              |

Tsurugiの開発初期の頃は、LTXは「長いトランザクションのうち、読み書きを伴うもの（read-write）」、RTXは「長いトランザクションのうち、読み取り専用のもの（read only）」と呼ばれていた。そのため、LTXとRTXの共通親クラスは`AbstractTgTxOptionLong`という名前（`Long`という接尾辞）になっている。

### コミットオプション

コミット時に指定するコミットオプションは、Tsubakuroでは`CommitStatus`という名前の列挙型である。
しかし「ステータス」だと「コミット中の状態を表す」あるいは「コミットの戻り値」のように思えてしまうので、Iceaxeでは`TgCommitType`というクラス名にした。

Tsubakuroの`commit`メソッドにはコミットオプションを引数に取らないオーバーロードがあるが、Iceaxeではコミットオプションを指定するメソッドのみとした。
`TsurugiTransactionManager`を使用していれば`TsurugiTransaction`の`commit`メソッドをアプリケーション開発者が直接呼ぶことは無いので、引数の無い`commit`メソッドの有無はアプリケーション開発者にとって関係ないだろう。

### `TsurugiSql`

SQL文やバインド変数定義を保持するクラスの共通親クラスは`TsurugiSql`というクラス名だが、元々はTsubakuroの`PreparedStatement`クラスをラップする目的だった（`TsurugiPreparedStatement`というクラス名だった）。

そのため、Iceaxe内やサンプルソースコード（iceaxe-examples）で`TsurugiSql`を代入する変数の名前は `ps` になっている（`ps`のままである）。

### `getLow`系メソッド

Iceaxeには、`getLowSesssion`や`getLowTransaction`メソッドといった、Tsubakuroのクラスを取得するメソッドが存在する。

メソッド名に`Low`が付いているのは、Iceaxeから見るとTsubakuroは低レベルAPIという位置付けな為。

これらのメソッドの中には可視性がpublicになっているものもあるが、実装の都合でpublicになっているだけであり、アプリケーション開発者に公開する目的のメソッドではない。

### タイムアウトの扱い

Tsubakuroのメソッドを呼び出して返ってくる`FutureResonse`から値を取得する為の`get`や`await`メソッドには、引数でタイムアウト時間を指定するものと指定しないもの（タイムアウトせずに無限に待つ）があるが、Iceaxeではタイムアウト時間を指定する`get`メソッドのみを使用している。

ただし、Iceaxeのデフォルトのタイムアウト時間は`Long.MAX_VALUE`ナノ秒（実質、無限）である。

### スレッドの使用

Tsubakuroはデータ受信の為に`Session`毎にスレッドを1つ作成する。

Iceaxeはスレッドを作成する箇所は無い。



## トランザクションオプションについて

### トランザクション種別

Tsurugiでは、トランザクション開始時にトランザクション種別（OCC・LTX・RTXのいずれか）を指定する。

トランザクション種別によって、どの時点のデータが読まれるかや、2つのトランザクションの処理が競合したときにどちらがシリアライゼーションエラー（リトライ可能なアボート）になるかが違ってくる。

- OCC（optimistic concurrency control）
  - 実行時間が短いトランザクション
    - Tsubakuroの`TransactionType.SHORT`に該当する。
    - 処理時間が20～40ミリ秒程度（DBの1 epochの長さ未満）で処理件数が少量（数百件程度）であれば、LTXより高速。
  - SQL実行時点のデータが読まれる。
    - 同条件のselectで（途中で他トランザクションによって値が更新・コミットされた時に）1回目と2回目で異なる値が読まれることもあるが、その場合はコミット時にシリアライゼーションエラーが発生する。
  - LTXと競合した場合、特殊な場合を除いてOCCがシリアライゼーションエラーになる。
  - OCC同士が競合した場合、後からコミットしようとした方がシリアライゼーションエラーになる。
- LTX（long transaction）
  - 実行時間が長いトランザクション
    - Tsubakuroの`TransactionType.LONG`に該当する。
  - 更新（insert, update, delete）する対象のテーブルを全てwrite preserveに指定する必要がある。
  - LTX開始時点のデータが読まれる。
    - selectのみのLTXも可能。OCCほどアボートしないが、コミット時に他LTXの完了待ちが発生しうる。
  - OCCと競合した場合、特殊な場合を除いてOCCがシリアライゼーションエラーになる。
  - LTX同士が競合した場合、後からトランザクション実行を開始した方がシリアライゼーションエラーになる。
- RTX（read only transaction）
  - 読み取り専用（selectのみ）のトランザクション
    - Tsubakuroの`TransactionType.READ_ONLY`に該当する。
  - 基本的にRTX開始時点のデータが読まれる。ただし、先に開始しているLTXがあった場合、そのLTX開始前のデータが読まれる。
    - 先に開始したLTXの開始時点で他に実行中のLTXがあったら、最悪の場合はそれら全ての開始前になるので、かなり古いデータが読まれることもある。
  - 他のトランザクションと競合しない。

### write preserve

write preserveは、LTXで更新（insert, update, delete）する対象のテーブルを指定する。

- write preserveに指定したテーブルを更新しなくてもエラーになるようなことは無い。
- 先に始まったLTXがwrite preserveで指定したテーブルを、後に始まったLTXが操作（select, insert, update, delete）した場合、後に始まったLTXは先に始まったLTXの完了まで待たされる。
  - 処理結果が競合していないかどうかは、優先度が高い（先に始まった）LTXが完了しないと判断できない為。
  - read areaを指定することで緩和される可能性はある。

### read area

read areaは、LTXやRTXで参照（select, insert, update, delete）する/しない対象のテーブルを指定する。

これにより、「他トランザクションの処理対象テーブルが競合しないこと」がすぐに判明する場合がある。

read areaを指定しなくてもテーブルを参照することは出来るが、トランザクションが完了するまでどのテーブルを参照するか判明しない為、他トランザクションが完了を待つことが発生しうる。

> **Note**
>
> 本来DBサーバー側としては参照しないテーブル（exclusive read area）を求めているのだが、実務上はクライアント側で参照しないテーブルを全て列挙するのは困難な為、参照するテーブル（inclusive read rea）を指定することがほとんどであろう。

## コミットについて

Tsurugiのトランザクション分離レベルはSERIALIZABLEである。

Tsurugiでは（Tsurugi以外でもトランザクション分離レベルがSERIALIZABLEであるRDBMSでは）、コミットが成功するまで、データに信頼性が無い（シリアライザブルであるという保証が無い）。
selectのみのトランザクションであっても、必ずコミットして成功することを確認しなければならない。

> **Note**
>
> トランザクション分離レベルREAD_COMMITTED（既存のほとんどのRDBMSのデフォルト）では、あるテーブルから全件selectし（1回目の取得）、そのテーブルが別トランザクションによってinsertまたはdelete・コミットされた後に再び全件selectすると（2回目の取得）、1回目より増減したデータが読まれる。すなわち、1回目と2回目で異なるデータがselectされる。
>
> SERIALIZABLEではこれは許されない（SERIALIZABLEでは、トランザクション内では常に同じデータが読まれなければならない）ので、こういう状態になるとシリアライゼーションエラーが発生する。
>
> 逆に言えば、コミットが成功したなら、シリアライゼーションエラーが起きなかったということである。

Tsurugiでは、SQLを実行した時やトランザクションをコミットした時にそのトランザクションの内容が他のトランザクションと競合していないか確認しており、競合した場合はシリアライゼーションエラー（リトライ可能なアボート）が発生することがある。

シリアライゼーションエラー（リトライ可能なアボート）が発生した場合は、トランザクション内で行った処理を先頭から再実行（リトライ）すれば、トランザクションが成功することが期待できる。

再実行の際はトランザクションオプションを変更してもよい。
（例えば、LTXで更新中のデータをOCCで読むとアボートするので、RTXで再実行するとLTX更新前のデータを読むのでアボートしない）

Iceaxeでは、シリアライゼーションエラー（リトライ可能なアボート）発生時の再実行は、`TsurugiTransactionManager`で自動的に実施することが出来る。



## DDLについて

create tableやdrop table等のDDLを実行する際もトランザクションのAPIを使用する。

DDLであっても、DBサーバー内部ではメタデータ登録の為にトランザクションIDが必要となる為。

そして、DBサーバーがシステムテーブルへメタデータを書き込む際にシリアライゼーションエラーが発生する可能性がある。

### `executeDdl`メソッド

DDLを実行するには、`TsurugiTransacion`の`executeDdl`メソッドを使用する。

更新系SQLとは実行するメソッドが異なるが、`executeDdl`メソッドの内部は更新系SQLの実行方法と全く同じ。
わずかな違いとして、更新系SQLでは処理件数を返す想定だが、DDLでは何も返さない。

### Iceaxeが対応しているデータ型

| create tableのデータ型   | Tsubakuroのデータ型（`AtomType`） | Iceaxeのデータ型（`TgDataType`）      | Javaのデータ型                    |
| ------------------------ | --------------------------------- | ------------------------------------- | --------------------------------- |
|                          | BOOLEAN                           | BOOLEAN                               | boolean（Boolean）                |
| INT                      | INT4                              | INT                                   | int（Integer）                    |
| BIGINT                   | INT8                              | LONG                                  | long（Long）                      |
| REAL                     | FLOAT4                            | FLOAT                                 | float（Float）                    |
| DOUBLE                   | FLOAT8                            | DOUBLE                                | double（Double）                  |
| DECIMAL                  | DECIMAL                           | DECIMAL                               | BigDecimal                        |
| CHAR<br />VARCHAR        | CHARACTER                         | STRING                                | String                            |
| BINARY<br />VARBINARY    | OCTET                             | BYTES                                 | byte[]                            |
|                          | BIT                               | BITS                                  | boolean[]                         |
| DATE                     | DATE                              | DATE                                  | LocalDate                         |
| TIME                     | TIME_OF_DAY                       | TIME                                  | LocalTime                         |
| TIMESTAMP                | TIME_POINT                        | DATE_TIME                             | LocalDateTime                     |
| TIME WITH TIME ZONE      | TIME_OF_DAY_WITH_TIME_ZONE        | OFFSET_TIME                           | OffsetTime                        |
| TIMESTAMP WITH TIME ZONE | TIME_POINT_WITH_TIME_ZONE         | OFFSET_DATE_TIME<br />ZONED_DATE_TIME | OffsetDateTime<br />ZonedDateTime |

> **Warning**
>
> 現時点では、WITH TIME ZONEにはタイムゾーンオフセットしか保持されない。（「Asia/Tokyo」のようなタイムゾーン情報は保持しない）
>
> IceaxeはZonedDateTimeに対応しているが、DB内ではタイムゾーンオフセットしか保持されないので、ZonedDateTimeで取得する場合はZoneIdを別途指定する必要がある。
>
> ```java
> var resultMapping = TgResultMapping.of(ZonedExapmleEntity.class)
>     .addZonedDateTime("date_time_with_timezone", ZonedExapmleEntity::setDateTime, ZoneId.of("Asia/Tokyo"));
> ```

テーブル名やカラム名の先頭にアンダースコア2つを付けるのは非推奨。（システムで予約されている）



## SQL（DML）の実行方法について

`TsurugiSql`の具象クラスは、SQLがselect文か更新系SQL（insert・update・delete文）か、SQL文をDBサーバーに事前に登録するか否かによって異なる。
こうした分け方になっているのは、TsubakuroのSQL実行方法の違いに依る。

内部的には、SQL文をDBサーバーに事前に登録する方式とは「Tsubakuroの`PreparedStatement`を使う方式」であり、事前に登録しない方式とは「実行時にSQL文を渡す`execute`系メソッドを使う方式」である。

| SQLの内容               | `TsurugiSql`の具象クラス      | TsubakuroでのSQL実行方法                      |
| ----------------------- | ----------------------------- | --------------------------------------------- |
| select文・事前登録なし  | `TsurugiSqlQuery`             | `transaction.executeQuery(sql)`               |
| select文・事前登録あり  | `TsurugiSqlPreparedQuery`     | `transaction.executeQuery(ps, parameter)`     |
| 更新系SQL・事前登録なし | `TsurugiSqlStatement`         | `transaction.executeStatement(sql)`           |
| 更新系SQL・事前登録あり | `TsurugiSqlPreparedStatement` | `transaction.executeStatement(ps, parameter)` |

### `TsurugiSql`が「`TsurugiSession`が同一の`TsurugiTransaction`」でしか使えない件について

少なくとも、`TsurugiSession`がクローズされたらそのインスタンスから作られた`TsurugiSql`はクローズされて使用不可になるので、別の`TsurugiSession`で作られた`TsurugiTransaction`では使用できない。

### `TsurugiQueryResult`・`TsurugiResultRecord`について

select文の実行結果のレコードを取得する`TsurugiQueryResult`と`TsurugiResultRecord`は、連動して、Tsubakuroの`ResultSet`の処理を行う。

Tsubakuroの`ResultSet`は`nextRow`メソッドによってレコードが存在するかどうかチェックし、存在している間ループする。また、`nextColumn`メソッドによってカラムが存在しているかどうかチェックし、存在している間ループしてカラムのデータを取得する。

Iceaxeでは、`TsurugiResultRecord`が1レコード分のデータを表し、`nextColumn`メソッドの処理を隠蔽している。
`TsurugiQueryResult`は`nextRow`メソッドの処理を隠蔽し、`Iterable<TsurugiResultRecord>`のような形をとっている。



## 例外ハンドリング

DBサーバーからエラーが返されると、Tsubakuroは`ServerException`を発生させる。

Iceaxeは、トランザクション実行中に発生した`ServerException`を`TsurugiTransactionException`にラップしてスローする。
トランザクション実行以外で発生した`ServerException`は`TsurugiIOException`にラップしてスローする。

`TsurugiTransactionException`や`TsurugiIOException`にはエラーコードを返す`getDiagnosticCode`メソッドがあるが、これは基本的に`ServerException`の`getDiagnosticCode`メソッドの値をそのまま返す。

