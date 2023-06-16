# Iceaxe使用方法（2023-06-13）

Iceaxeの使用方法（Tsurugiのデータベース（Tsurugi DB）に対してSQLを実行する方法）の概要を説明する。

- IceaxeはJava11以降に対応。
- [iceaxe-examples]にIceaxeを使った実装例がある。
- Iceaxeの概要や当ドキュメントで使っている用語については[about-iceaxe.md](about-iceaxe.md)を参照。
- Iceaxeの内部構造に関する補足については[iceaxe-internal.md](iceaxe-internal.md)を参照。

[iceaxe-examples]:../modules/iceaxe-examples/src/main/java/com/tsurugidb/iceaxe/example



## Iceaxeの主要なクラス

Iceaxeを用いてTsurugi DBでSQLを実行する際に使用するクラスは、主に以下のようなもの。

| クラス                                                       | 説明                                                         | 使用目的                                                     | クローズ |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ | -------- |
| `com.tsurugidb.iceaxe`<br />`TsurugiConnector`               | Tsurugi DBのエンドポイント（接続情報）と認証情報を保持するクラス | `TsurugiSession`を生成する。                                 | -        |
| `com.tsurugidb.iceaxe.session`<br />`TsurugiSession`         | Tsurugi DBと接続し、通信を行うクラス                         | `TsurugiSql`や`TsurugiTransaction`を生成する。               | **要**   |
| `com.tsurugidb.iceaxe.session`<br />`TgSessionOption`        | `TsurugiSeesion`のオプションを保持するクラス                 | `TsurugiSession`の生成時に指定する。                         |          |
| `com.tsurugidb.iceaxe.transaction`<br />`TsurugiTransction`  | トランザクションを表すクラス                                 | SQLを実行し、コミットまたはロールバックを行う。              | 要       |
| `com.tsurugidb.iceaxe.transaction.option`<br />`TgTxOption`  | トランザクション開始時のオプションを表すクラス               | `TsurugiTransaction`生成時に指定する。                       | -        |
| `com.tsurugidb.iceaxe.transaction`<br />`TgCommitType`       | トランザクションコミット時のオプションを表す列挙型           | `TsurugiTransaction`のコミット時に指定する。                 | -        |
| `com.tsurugidb.iceaxe.transaction.manager`<br />`TsurugiTransactionManager` | `TsurugiTransaction`のライフサイクルを管理するクラス         | シリアライゼーションエラー発生時に自動的にトランザクションを再実行する。 | -        |
| `com.tsurugidb.iceaxe.transaction.manager`<br />`TgTmSetting` | `TsurugiTransactionManager`の設定を保持するクラス            | `TsurugiTransactionManager`で `TsurugiTransaction`を生成する際のオプション。 | -        |
| `com.tsurugidb.iceaxe.sql`<br />`TsurugiSql`                 | SQL文や`TgParameterMapping`・`TgResultMapping`を保持するクラス | `TsurugiTransaction`でSQLを実行する。                        | 要       |
| `com.tsurugidb.iceaxe.sql.parameter`<br />`TgParameterMapping` | バインド変数（SQL文の中に変数を埋め込み、SQL実行時にその変数に対してSQL文の外から値を代入する仕組み）の定義を保持するクラス | `TsurugiSql`生成時に指定する。                               | -        |
| `com.tsurugidb.iceaxe.sql.result`<br />`TgResultMapping`     | select文の実行結果をアプリケーション開発者が用意するEntityクラスに変換する為の定義を保持するクラス | `TsurugiSql`生成時に指定する。                               | -        |
| `com.tsurugidb.iceaxe.sql.result`<br />`TsurugiQueryResult`  | select文の実行結果を管理するクラス                           | select文が返すレコードを取得する。                           | 要       |
| `com.tsurugidb.iceaxe.sql.result`<br />`TsurugiStatementResult` | 更新系SQLの実行結果を管理するクラス                          | 更新系SQLの実行が完了したかどうかを確認する。                | 要       |

- Iceaxeのほとんどのクラスは、初期設定処理やクローズ処理以外はスレッドセーフ。
  - `TsurugiQueryResult`はスレッドセーフではない。
- `TsurugiSession`はTsurugiのDBサーバーと接続している為、使い終わったら**必ず**クローズする必要がある。
  - `TsurugiSession`以外でクローズする必要があるクラスでは、明示的にクローズされていない場合、そのインスタンスの生成元がクローズされた時にクローズされる。（`TsurugiSession`がクローズされたら、そこから生成されたインスタンスは再帰的に全てクローズされる）



## Iceaxeの使用手順

Iceaxeを用いてTsurugi DBでSQLを実行する手順は、概ね以下のようになる。

1. Tsurugi DBのエンドポイント（接続情報）と認証情報を指定して`TsurugiConnector`を生成する。
2. `TsurugiConnector`から`TsurugiSession`を生成する（DBに接続する）。
3. `TsurugiSession`からSQLを表す`TsurugiSql`を生成する。
4. `TsurugiSession`から`TsurugiTransactionManager`を生成する。
5. `TsurugiTransactionManager`を使ってトランザクションを実行する。
   1. （`TsurugiTransactionManager`が`TsurugiTransaction`を生成する）
   2. `TsurugiTransaction`を用いて`TsurugiSql`を実行する（SQLを実行する）。
   3. （`TsurugiTransactionManager`が`TsurugiTransaction`をコミットまたはロールバックし、クローズする）
6. 使い終わった`TsurugiSql`をクローズする。
7. `TsurugiSession`をクローズする。

なお、`TsurugiSql`と`TsurugiTransaction`を生成する順序は任意。
トランザクションの実行前に`TsurugiSql`を生成して実行後にクローズしても、
トランザクションの実行中に`TsurugiSql`を生成・クローズしても問題ない。

### 例

```java
// TsurugiConnector生成
var endpoint = URI.create("tcp://localhost:12345");
var credential = new UsernamePasswordCredential("user", "password");
var connector = TsurugiConnector.of(endpoint, credential);

// TsurugiSession生成
var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
try (var session = connector.createSession(sessionOption)) {

    // TsurugiSql生成
    var sql = "update TEST set BAR = 1 where FOO = 123";
    try (var ps = session.createStatement(sql)) {
        
        // TsurugiTransactionManager生成
        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        var tm = session.createTransactionManager(setting);

        // トランザクションを実行
        tm.execute(transaction -> {
            // SQLを実行
            transaction.executeAndGetCount(ps);
        });
    }
}
```




## `TsurugiConnector`

どのTsurugi DBに接続するかという情報と接続方法は『エンドポイント』と呼ばれるURIで指定する。
`TsurugiConnector`はエンドポイントと認証情報を保持するクラス。

- `TsurugiConnector`は自分自身のインスタンス生成メソッドを使って生成する。
- `TsurugiConnector`は`TsurugiSession`を生成することに使用する。
- `TsurugiConnector`は`TsurugiSession`生成に関してスレッドセーフ。

> **Note**
>
> エンドポイントの内容は、Iceaxeが使用している通信ライブラリーTsubakuroの仕様に準ずる。[iceaxe-internal.md](iceaxe-internal.md)を参照。
>
> - TCP接続の例 … `tcp://localhost:12345`
> - IPC接続の例 … `ipc:tateyama`

### `TsurugiConnector`を生成する例

`TsurugiConnector`インスタンスは`TsurugiConnector`クラスの`of`メソッドで生成する。

```java
import java.net.URI;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

var endpoint = URI.create("tcp://localhost:12345");
var credential = new UsernamePasswordCredential("user", "password");
var connector = TsurugiConnector.of(endpoint, credential);
```

`TsurugiConnector`の生成時には、エンドポイントの他に、`TsurugiSession`を生成するのに必要な認証情報（`Credential`）を指定することが出来る。

認証情報をここで指定せず、`TsurugiSession`を生成する`createSession`メソッドの引数として渡すことも出来る。

```java
var connector = TsurugiConnector.of(endpoint);
try (var session = connector.createSession(credential)) {
    ～
}
```

`TsurugiConnector`を生成する例は、[iceaxe-examples]の`Example01Connector`を参照。
`Credential`の例は、[iceaxe-examples]の`Example01Credential`を参照。

> **Warning**
>
> 現時点ではDBサーバー側で認証が実装されていない（認証を行わない）ので、どんな`Credential`を指定しても接続可能。



## `TsurugiSession`

`TsurugiSession`は、Tsurugi DBに接続して通信（データ送受信）を行うクラス。

> **Note**
>
> Iceaxeでは、『セッション』という用語はDB接続を表す。
>
> 例えばTCP接続の場合、ひとつの`TsurugiSession`インスタンスが（Iceaxeが使用している通信ライブラリーTsubakuro内で）ひとつのTCPソケットを保持する。

- `TsurugiSession`は`TsurugiConnector`から生成する。
  - 同時に生成できるセッション数にはDBサーバー側に上限がある。
  - 使用終了後に**必ず**クローズする必要がある。
- `TsurugiSession`から`TsurugiSql`や`TsurugiTransaction`を生成する。
- `TsurugiSession`の`create`系メソッド（`TsurugiSql`の生成や`TsurugiTransaction`の生成等）はスレッドセーフ。（それぞれ複数をマルチスレッドで生成可能）



`TsurugiSql`や`TsurugiTransaction`は、DBサーバーとの通信に（生成元の）`TsurugiSession`を使用する。

> **Note**
>
> `TsurugiSql`や`TsurugiTransaction`の操作自体は並列に実行可能なのだが、Iceaxeが使用している通信ライブラリーTsubakuro内では、通信データをキューに入れ、順番に送信する。
> したがって、ひとつの`TsurugiSession`から大量に`TsurugiTransaction`を生成してSQLを並列に実行すると、キューが詰まって実行が遅くなる可能性がある。


### `TsurugiSession`を生成する例

`TsurugiSession`インスタンスは`TsurugiConnector`の`createSession`メソッドで生成する。

`TsurugiSession`インスタンスを使い終わったら、**必ず**クローズする必要がある。
（`TsurugiSession`から生成された`TsurugiSql`や`TsurugiTransaction`が明示的にクローズされなかった場合は`TsurugiSession`のクローズ時にクローズされるが、`TsurugiSession`が明示的にクローズされなかった場合に暗黙にクローズする仕組みは無い）

```java
import java.util.concurrent.TimeUnit;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

var sessionOption = TgSessionOption.of().setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
try (var session = connector.createSession(sessionOption)) {
    ～
}
```

`TsurugiSession`の生成時にセッションオプション（`TgSessionOption`）を指定する。
`TgSessionOption`には、通信のタイムアウト時間や「`TsurugiTransaction`のコミット時に指定するコミットオプション」のデフォルト値を設定することが出来る。

`TgSessionOption`を指定する必要がない場合は、引数なしの`createSession`メソッドを利用できる。

```java
try (var session = connector.createSession()) {
    ～
}
```

`TgSessionOption`や`TsurugiSession`を生成する例は、[iceaxe-examples]の`Example02Session`を参照。




## `TsurugiTransaction`

`TsurugiTransaction`は、1回分のトランザクションを管理するクラス。

> **Note**
>
> Iceaxeの基本的な使用方法としては、`TsurugiTransaction`インスタンスのライフサイクル（生成・コミット・ロールバック・クローズ）は後述の`TsurugiTransactionManager`で管理する（アプリケーション開発者が直接ライフサイクルに関与しない）ことを想定しているが、`TsurugiTransaction`インスタンスの生成をアプリケーション開発者が直接行うことを禁止しているわけではない。

- `TsurugiTransaction`は`TsurugiSession`から生成する。
  - 使用終了後にクローズする必要がある。（明示的にクローズしない場合、`TsurugiSession`のクローズ時にクローズされる）
  - `TsurugiTransactionManager`を使用する場合は、その内部で生成・クローズされる。
- SQL（`TsurugiSql`）を実行する際に`TsurugiTransaction`を使用する。
- `TsurugiTransaction`の状態取得メソッドや`execute`系メソッド（`TsurugiSql`の実行）・コミット・ロールバックはスレッドセーフ。

トランザクションは`TsurugiTransaction`生成と同時に開始され、コミットまたはロールバックで終了する。その後`TsurugiTransaction`をクローズする。

> **Note**
>
> `TsurugiTransaction`をコミット・ロールバックしなかった場合は、クローズ時にロールバック扱いとなる。

`TsurugiTransaction`生成時にはトランザクションオプション（`TgTxOption`）を指定する。
コミット時にはコミットオプション（`TgCommitType`）を指定する。

コミット・ロールバックは一度しか実行できない。

- コミット完了後にコミットやロールバックを呼んでも無視される。
  - コミット完了後にロールバックを呼んでも無視するのは、try文の本体でコミットしてfinally節でロールバックを呼んでも問題無いようにする為。
- ロールバック完了後にロールバックを呼んでも無視される。
- ロールバック完了後にコミットを呼ぶと例外が発生する。

> **Warning**
>
> 現時点では、Iceaxeが使用している通信ライブラリーTsubakuroでは、コミットやロールバック処理中に例外が発生した場合、その後にコミットやロールバックを呼んでも無視される。

コミットやロールバック完了後にSQLを実行しようとすると、例外が発生する。

トランザクションの実行中にDBサーバー側の要因によってエラー（例えばinsertの一意制約違反）が発生すると、その`TsurugiTransaction`はそれ以上使用できなくなる。（トランザクションの状態がinactiveになる）

### `TgTxOption`

`TgTxOption`は、`TsurugiTransction`生成時に指定するトランザクションオプションを表すクラス。

- `TgTxOption`は、トランザクション種別（OCC・LTX・RTX）に応じた`of`系メソッドで生成する。
  - LTXでDDLを実行する場合は`ofDDL`メソッドで生成できる。
- `TgTxOption`は`TsurugiTransaction`生成時に指定する。
- `TgTxOption`はスレッドセーフ。

> **Note**
>
> トランザクション種別の概要は、[iceaxe-internal.md](iceaxe-internal.md)を参照。

#### `TgTxOption`の例


```java
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var occ = TgTxOption.ofOCC();
var ltx = TgTxOption.ofLTX("table1", "table2"); // write preserve table
var rtx = TgTxOption.ofRTX().label("example");
var ddl = TgTxOption.ofDDL(); // LTXでDDLを実行する場合（ofLTX().includeDdl(true)と同じ）
```

`ofLTX`メソッドでは、引数でwrite preserveを指定できる。

#### `TgTxOption`に指定できるパラメーター

- OCC・LTX・RTX共通
  - label
    - トランザクションのラベル（人間が見る為のもの）
      - `TsurugiTransaction`で取得できる情報のひとつとして、デバッグ等に利用できる。
- LTX・RTX共通
  - priority
    - トランザクションの優先度（排他的実行の制御）
    - `TransactionPriority`列挙型で指定する。
      - `TRANSACTION_PRIORITY_UNSPECIFIED`（デフォルト）
        - `EXCLUDE`のトランザクションが実行中だったら、実行しない（自分がfailする）。
      - `INTERRUPT`
        - 実行中の他トランザクションをfailさせてから実行する。ただし、`EXCLUDE`のトランザクションが実行中だったら、実行しない（自分がfailする）。
      - `WAIT`
        - 実行中のトランザクションがあったら、それら全てが終わるのを待ってから実行する。
        - 待っている間、新しいトランザクションは実行できない（failさせる）。
      - `EXCLUDE`有無
        - `EXCLUDE`なし
          - 自分の実行中に新しいトランザクションを実行可能。
        - `EXCLUDE`あり
          - 自分の実行中に新しいトランザクションは実行できない（failさせる）。
- LTXのみ
  - include ddl
    - DDLを実行するか否か
  - write preserve
    - 更新対象の（insert/update/deleteを実行する対象の）テーブル名
  - inclusive read area
    - 参照する（select/insert/update/deleteを実行する対象の）テーブル名
  - exclusive read area
    - 参照しない（select/insert/update/deleteを実行しない対象の）テーブル名

> **Warning**
>
> 現時点では、priority, inclusive read area, exclusive read areaはDBサーバー側で未実装なので、指定することは出来るが、効果は無い。

`TgTxOption`を生成する例は、[iceaxe-examples]の`Example03TxOption`を参照。

### `TgCommitType`

`TgCommitType`は、`TsurugiTransaction`の`commit`メソッドに指定する列挙型。

DBサーバー内でどこまで処理したら`commit`メソッドから制御が返るかを表す。

- `DEFAULT`
  - DBサーバー側の設定に従う。
- `ACCEPTED`
  - コミット操作が受け付けられた。
- `AVAILABLE`
  - コミットデータが他トランザクションから見えるようになった。
- `STORED`
  - コミットデータがローカルディスクに書かれた。
- `PROPAGATED`
  - コミットデータが適切な全てのノードに伝播された。

```java
import com.tsurugidb.iceaxe.transaction.TgCommitType;

transaction.commit(TgCommitType.DEFAULT);
```

> **Warning**
>
> 現時点では、コミットオプションはDBサーバー側で未実装。何かを指定する必要はあるが、効果は無い。



### `TsurugiTransaction`の例

`TsurugiTransaction`インスタンスは`TsurugiSession`の`createTransaction`メソッドで生成する。
`TsurugiTransaction`インスタンスを使い終わったら、クローズする必要がある。（明示的にクローズしない場合、`TsurugiSession`のクローズ時にクローズされる）

```java
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;

var txOption = TgTxOption.ofLTX("table1", "table2");
try (var transaction = session.createTransaction(txOption)) {
    try {
        ～
        transaction.commit(TgCommitType.DEFAULT);
    } catch (Throwable t) {
        transaction.rollback();
    }
}
```

> **Note**
>
> `TsurugiTransactionManager`の内部では、上記のような`TsurugiTransaction`生成・コミット・ロールバック・クローズ処理を行っている。



### `TsurugiTransaction`の状態取得

`TsurugiTransaction`の状態を取得するメソッドのうち、主なもの。

| メソッド                                            | 取得内容                                                     | 備考                                                         |
| --------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `int getIceaxeTxId()`                               | `TsurugiTransaction`に付けられた番号                         | `TsurugiTransaction`に関してIceaxe（JavaVM）内でユニークな番号。デバッガー等で識別するときに利用できる |
| `TgTxOption getTransactionOption()`                 | `TsurugiTransaction`生成時に指定された`TgTxOption`           |                                                              |
| `TsurugiTransactionManager getTransactionManager()` | `TsurugiTransaction`を生成した`TsurugiTransactionManager`    | `TsurugiTransactionManager`経由でない場合は`null`            |
| `int getIceaxeTmExecuteId()`                        | `TsurugiTransactionManager`の `execute`メソッドを実行する際に付けられた番号 | `TsurugiTransaction`実行に関してIceaxe（JavaVM）内でユニークな番号。シリアライゼーションエラーによって再実行しても変わらないので、一連のトランザクションを識別したいときに利用できる。<br />`TsurugiTransactionManager`経由でない場合は`0` |
| `int getAttempt()`                                  | 試行番号（`TsurugiTransactionManager`での再実行回数）        | 初回は0で、シリアライゼーションエラーによって再実行する度に増えていく。<br />`TsurugiTransactionManager`経由でない場合は常に`0` |
| `String getTransactionId()`                         | DB側が採番したトランザクションID                             | DBサーバー側のログでもこのトランザクションIDが出力されるので、紐付けるのに利用できる。<br />トランザクションIDは`toString`メソッドが返す文字列内にもあるが、そちらは、DBサーバーから取得していない時点では`null` |
| `boolean available()`                               | `TsurugiTransaction`が有効かどうか（DBサーバーと通信可能かどうか） | DBサーバー側でエラー（一意制約違反等）が発生した場合にトランザクションがinactiveになることがあるが、それはこのメソッドでは検知できない |
| `boolean isCommitted()`                             | `TsurugiTransaction`がコミットされたかどうか                 |                                                              |
| `boolean isRollbacked()`                            | `TsurugiTransaction`がロールバックされたかどうか             |                                                              |



## `TsurugiTransactionManager`

`TsurugiTransactionManager`は、`TsurugiTransaction`のライフサイクルを管理するクラス。
`TsurugiTransaction`内でシリアライゼーションエラー（リトライ可能なアボート）が発生したときに自動的に再実行する機能を持つ。

- `TsurugiTransactionManager`は`TsurugiSession`から生成する。
  - `TsurugiTransactionManager`は`Closeable`ではない（クローズする必要は無い）。
- `TsurugiTransactionManager`内部で`TsurugiTransaction`を生成する。
- `TsurugiTransactionManager`の`execute`系メソッド（トランザクションの実行）はスレッドセーフ。

> **Note**
>
> `TsurugiTransactionManager`内部で`TsurugiTransaction`を生成する際には、`TsurugiTransactionManager`の生成に使われた`TsurugiSession`（DB接続）を使用する。

### `TsurugiTransactionManager`の例

`TsurugiTransactionManager`インスタンスは`TsurugiSession`の`createTransactionManager`メソッドで生成する。

`TsurugiTransactionManager`を生成する際か、トランザクションを実行する為に`execute`メソッドを呼び出す際のどちらかで、`TgTmSetting`（`TsurugiTransactionManager`の設定）を渡す必要がある。

`TgTmSetting`では、`TsurugiTransaction`生成時に使用する`TgTxOption`を定義する`TgTmTxOptionSupplier`や、コミット時に使用する`TgCommitType`等を設定しておくことが出来る。

アプリケーション開発者は`TsurugiTransactionManager`の`execute`メソッドを呼び出してトランザクションを実行する。

```java
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionSupplier;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var supplier = TgTmTxOptionSupplier.ofAlways(TgTxOption.ofOCC());
var setting = TgTmSetting.of(supplier).commitType(TgCommitType.DEFAULT);
var tm = session.createTransactionManager(setting);
tm.execute(transaction -> {
    ～
});
```

`TsurugiTransactionManager`の`execute`メソッドを呼ぶと、`TsurugiTransactionManager`内部で`TsurugiTransaction`が生成される。

`execute`メソッドに渡す関数が、トランザクション1回分の処理となる。

- この関数では、`TsurugiTransaction`を受け取り、それを使って`TsurugiSql`を実行する。
- この関数が正常に終了すると、`TsurugiTransactionManager`は`TsurugiTransaction`をコミットする。
  - ただし、関数内で`TsurugiTransaction`に対して明示的にロールバックを実行した場合、`TsurugiTransactionManager`はコミットを実行しない。
- この関数内で例外が発生すると、`TsurugiTransactionManager`は`TsurugiTransaction`をロールバックする。
- シリアライゼーションエラー（リトライ可能なアボート）の例外が発生すると、`TsurugiTransactionManager`は新しい`TsurugiTransaction`を生成して、この関数を再度実行する。そのため、この関数は冪等な処理でなければならない。

`TsurugiTransactionManager`の例は、[iceaxe-examples]の`Example04TransactionManager`を参照。
`TsurugiTransactionManager`で`TgCommitType`を指定しておく例は、[iceaxe-examples]の`Example91CommitType`を参照。

### `TgTmTxOptionSupplier`

`TsurugiTransactionManager`を使用する場合は、初回や再実行時の`TsurugiTransaction`生成に使用する`TgTxOption`を`TgTmTxOptionSupplier`で定義し、`TgTmSetting`に入れておく。

どのようなエラーコードをシリアライゼーションエラーとするかの判定も`TgTmTxOptionSupplier`に設定できる。

シリアライゼーションエラーによってトランザクションを再実行する際に、`TgTmTxOptionSupplier`で設定されることになる実行回数を超えた場合、`TsurugiTransactionManager`は`TsurugiTmRetryOverIOException`（リトライ回数オーバーの例外）をスローする。

`TgTmTxOptionSupplier`を生成するメソッドは以下のようなものがある。

| 生成メソッド                                       | 説明                                                         | 実行回数                            |
| -------------------------------------------------- | ------------------------------------------------------------ | ----------------------------------- |
| `of(txOption)`                                     | 初回の`TgTxOption`を指定する。                               | 1回（再実行はしない）               |
| `of(txOptions...)`                                 | `TgTxOption`の一覧を指定する。n個目の`TgTxOption`がn回目の実行時に使われる。 | 指定された`TgTxOption`の個数        |
| `ofAlways(txOption)`                               | 常に指定された`TgTxOption`で実行される。                     | `Integer.MAX_VALUE`回（実質、無限） |
| `ofAlways(txOption, attemptMaxCount)`              | 常に指定された`TgTxOption`で実行される。                     | `attemptMaxCount`回                 |
| `of(txOption1, size1, txOption2, size2)`           | `txOption1`を`size1`回実行した後で、`txOption2`を`size2`回実行する。 | `size1 + size2`回                   |
| `ofOccLtx(occOption, occSize, ltxOption, ltxSize)` | `occOption`を`occSize`回実行した後で、`ltxOption`を`ltxSize`回実行する。`occOption`にはOCCを、`ltxOption`にはLTXまたはRTXを指定する。<br />OCCが他LTXのwrite preserveの範囲に入った事によって再実行する場合は`ltxOption`に切り替える。 | `occSize + ltxSize`回               |

`TgTmSetting`にも同様のメソッドがあり、そちらを使うと、下記の例のように`TgTmTxOptionSupplier`の生成がソースコード上は省略できる。（`TgTmSetting`の内部で`TgTmTxOptionSupplier`を生成している）

```java
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
var tm = session.createTransactionManager(setting);
```

`TgTmTxOptionSupplier`および`TgTmSetting`の例は、[iceaxe-examples]の`Example04TmSetting`を参照。



## DDLの実行方法

create tableやdrop table等のDDLを実行する際も`TsurugiTransaction`を使用する。

しかし、DDLはトランザクションの管理外である。
例えば、create tableが成功した時点で（コミットしなくても）テーブルは作られ、ロールバックしてもテーブルは消えない。
（ただし、現在未対応のcreate temporary tableでは、トランザクションが終了したらテーブルが消える予定）

DDLの実行でもシリアライゼーションエラーが発生する可能性があるので、再実行できるようにする必要がある。

> **Warning**
>
> 将来、DDLの実行もトランザクショナルになる予定。

> **Warning**
>
> DDLを実行するトランザクションでは、DMLを実行してはならない。現時点では、DBサーバー側の動作保証外。

> **Warning**
>
> 別トランザクションであっても、DDLとDMLを並行して実行してはならない。現時点では、DBサーバー側の動作保証外。
>
> DML実行中のテーブルをdropした場合等に、DBサーバーが無応答になったりクラッシュしたりすることがある。

### DDLを実行する例

DDLは`TsurugiTransaction`の`executeDdl`メソッドで実行できる。

LTXでDDLを実行する場合、write preserve（更新対象テーブル）を指定する必要は無いが、include ddlを指定する必要がある。
`TgTxOption`の`ofDDL`メソッドを使うと、include ddlが指定されたLTXのトランザクションオプションが作られる。

```java
var setting = TgTmSetting.ofAlways(TgTxOption.ofDDL());
var tm = session.createTransactionManager(setting);
tm.execute(transaction -> {
    transaction.executeDdl("create table TEST(FOO int primary key, BAR bigint, ZZZ varchar(10))");
});
```

1回のトランザクションで1個しかDDLを実行しない場合は、下記の例のように`TsurugiTransactionManager`の`executeDdl`メソッドを利用することも出来る。
通常は`TsurugiTransactionManager`を生成する際に`TgTmSetting`を指定するが、`TsurugiTransactionManager`の`executeDdl`メソッドを使う場合は省略可能。（省略した場合はwrite preserve無しのLTXとなる）

```java
var tm = session.createTransactionManager();
tm.executeDdl("create table TEST(FOO int primary key, BAR bigint, ZZZ varchar(10))");
```

DDLの例は、[iceaxe-examples]の`Example11Ddl`を参照。



## SQL（DML）の実行方法

`TsurugiSql`は、SQL文を管理するクラス。`TsurugiSql`を使ってSQLを実行する。

- `TsurugiSql`は`TsurugiSession`から生成する。
  - 使用終了後にクローズする必要がある。（明示的にクローズしない場合、`TsurugiSession`のクローズ時にクローズされる）
- `TsurugiTransaction`の`execute`系メソッドに`TsurugiSql`を渡してSQLを実行する。
  - `TsurugiSql`は、「`TsurugiSql`が作られたのと同じ`TsurugiSession`」から作られた`TsurugiTransaction`に対してしか実行できない。
- `TsurugiSql`（具象クラスを含む）は、SQL実行に関してスレッドセーフ。

SQLがselect文か更新系SQL（insert・update・delete文）か、SQL文をDBサーバーに事前に登録するか否かによって、`TsurugiSql`の具象クラスやそれを生成するメソッドが異なる。

> **Note**
>
> SQL文をDBサーバーに事前に登録する方式は、SQL文の解釈が登録時に一度行われるだけなので、何度も同じSQLを実行する場合に使用すると良い。
>
> SQL文をDBサーバーに事前に登録しない方式だと、SQLを実行する度にSQL文をDBサーバーに渡すので毎回SQL文の解釈が行われることになるが、一度しか実行しない場合はこちらの方式で問題ない。

SQL文をDBサーバーに事前に登録する方式の場合、バインド変数（SQL文の中に変数を埋め込み、SQL実行時にその変数に対してSQL文の外から値を代入する仕組み）の定義が必須。
`TsurugiSession`の`createQuery`や`createStatement`メソッドを呼び出すことにより、SQL文やバインド変数の定義がDBサーバーに登録される。

SQL文をDBサーバーに事前に登録しない方式の場合、バインド変数は使用できない。
しかし`TsurugiSession`の`createQuery`や`createStatement`メソッドを呼び出して`TsurugiSql`を生成するという点は同じ。

| SQLの内容               | `TsurugSql`生成メソッド                             | 返ってくる具象クラス          |
| ----------------------- | --------------------------------------------------- | ----------------------------- |
| select文・事前登録なし  | `createQuery(sql, resultMapping)`                   | `TsurugiSqlQuery`             |
| select文・事前登録あり  | `createQuery(sql, parameterMapping, resultMapping)` | `TsurugiSqlPreparedQuery`     |
| 更新系SQL・事前登録なし | `createStatement(sql)`                              | `TsurugiSqlStatement`         |
| 更新系SQL・事前登録あり | `createStatement(sql, parameterMapping)`            | `TsurugiSqlPreparedStatement` |

- 生成メソッドの引数
  - `sql`（必須）
    - SQL文の文字列
  - `parameterMapping`（SQL文をDBサーバーに事前に登録する場合は必須）
    - バインド変数の変数名とデータ型の定義
  - `resultMapping`（select文のみ）
    - select文の実行結果のレコードをアプリケーション開発者が用意するEntityクラスに変換する為の定義
    - 省略時は、Iceaxeが用意している`TsurugiResultEntity`に変換される。

### `TsurugiSql`の実行方法

`TsurugiTransaction`の`execute`系メソッドに`TsurugiSql`を渡してSQLを実行する。
バインド変数が定義されている`TsurugiSql`の場合は、バインド変数に代入する値をパラメーターとして渡す。

| 処理内容                   | メソッド               | 説明                                                 |
| -------------------------- | ---------------------- | ---------------------------------------------------- |
| select文・全件取得         | `executeAndGetList`    | 全レコードの`List`を返す                             |
| select文・1件取得          | `executeAndFindRecord` | 1レコードを`Optional`で返す                          |
| select文・1件ずつ処理      | `executeAndForEach`    | レコードを処理する関数を渡す                         |
| 更新系SQL                  | `executeAndGetCount`   | 処理件数を返す（という想定だが、現在は常に-1を返す） |
| select文・SQL実行結果取得  | `executeQuery`         | SQL実行結果（`TsurugiQueryResult`）を返す            |
| 更新系SQL・SQL実行結果取得 | `executeStatement`     | SQL実行結果（`TsurugiStatementResult`）を返す        |

> **Warning**
>
> `executeAndGetCount`メソッドは更新系SQLの処理件数を返す想定だが、現時点ではDBサーバーから処理件数が渡されないので、常に-1を返している。

#### `executeQuery`・`executeStatement`メソッド

`TsurugiSql`を実行する基本的な方法は、`executeQuery`・`executeStatement`メソッドを使って`TsurugiSqlResult`（SQL実行結果）を取得する方法である。

- select文の場合、`TsurugiSqlResult`の具象クラスである`TsurugiQueryResult`からselect結果のレコードを取得する。
- 更新系SQLの場合、`TsurugiSqlResult`の具象クラスである`TsurugiStatementResult`の`getUpdateCount`メソッドを呼び出して、更新が成功したかどうかを確認する。

`TsurugiQueryResult`・`TsurugiStatementResult`は、使い終わったらクローズする必要がある。（明示的にクローズしない場合は、`TsurugiTransaction`のコミット前やロールバック前、あるいはクローズ時にクローズされる）

#### `executeAnd`系メソッド

`executeAnd`系メソッドを使うと、`TsurugiSqlResult`を介さずに結果を取得できる。

> **Note**
>
> `executeAnd`系メソッドは内部で`TsurugiSqlResult`を操作しており、更新の成功可否チェックや`TsurugiSqlResult`のクローズも実施する。
>
> このため、基本的には`executeQuery`・`executeStatement`メソッドより`executeAnd`系メソッドの使用を推奨する。

`TsurugiTransactionManager`にも同名の`executeAnd`系メソッドがあり、1回のトランザクションで1個しかSQLを実行しない場合は、こちらを利用することも出来る。

#### SQLの実行完了の確認

トランザクション内でSQLを順次実行したい場合は、SQLの実行完了を確認してから次のSQLを実行開始する必要がある。
SQLの実行完了を確認せずに次のSQLを実行すると、そのSQLはDBサーバー内部では前のSQLと並列に処理される可能性がある。

SQLの実行完了を確認する方法は、以下の通り。

- select文の場合、全件読み終わること
  - `executeAndGetList`, `executeAndForEach`メソッドは全件読むので、メソッドが終了すればSQLの実行が完了している。
  - `executeAndFindRecord`メソッドは1件しか読まないので、メソッドが終了してもSQLの実行が完了している保証はない。
- 更新系SQLの場合、更新の成功可否をチェックすること（更新が成功したなら、SQLの実行が完了している）
  - `executeAndGetCount`メソッドは更新の成功可否をチェックするので、メソッドが終了すればSQLの実行が完了している。

----

select文の例は[iceaxe-examples]の`Example31Select`, `Example32Count`を参照。
更新系SQLの例は[iceaxe-examples]の`Example21Insert`, `Example41Update`を参照。



### SQL実行の例（SQL文の事前登録を行わない方式の更新系SQLの例）

SQL文をDBサーバーに事前に登録しない方式で、更新系SQLを実行する例。

```java
var sql = "update TEST set BAR = 1 where FOO = 123";
try (var ps = session.createStatement(sql)) {
    tm.execute(transaction -> {
        transaction.executeAndGetCount(ps);
    });
}
```

`TsurugiSession`の`createStatement`メソッドで引数の`parameterMapping`を省略すると、SQL文をDBサーバーに事前に登録しない方式で`TsurugiSql`が作られる。

`TsurugiTransaction`の`executeAndGetCount`メソッド等に`TsurugiSql`を渡して更新系SQLを実行する。

> **Note**
>
> 更新系SQLを実行するメソッドには、`executeStatement`と`executeAndGetCount`がある。
>
> 後者は処理件数を返すメソッドなので、処理件数が不要な場合は前者で実行したくなるかもしれないが、前者は`TsurugiStatementResult`を返すメソッドなので戻り値を無視するのは不適切。
>
> `TsurugiStatementResult`はクローズする必要があるが、`executeStatement`メソッドの戻り値（`TsurugiStatementResult`）を無視すると、クローズが漏れてしまう。
> クローズが漏れても`TsurugiTransaction`クローズ時にクローズされるが、SQLの実行完了を待たないことになるので、SQLが順次実行されることを期待していた場合は予期せぬ挙動になる可能性がある。
>
> そのため、基本的には`executeAndGetCount`メソッドの使用を推奨する。（`executeAndGetCount`メソッドは内部で更新の成功可否をチェックし、`TsurugiStatementResult`をクローズする為）



### select文の実行方法（SQL文のDBサーバーへの事前登録有無に関わらず共通）

select文は`TsurugiTransaction`の`executeAndGetList`メソッド等で実行するが、select文を実行するとDBから（複数の）レコードが返ってくる。
返ってきたレコードは、アプリケーション開発者が用意するEntityクラス（setterメソッドを持つクラス）に変換される。
`TgResultMapping`はこの変換の定義を行うクラス。

アプリケーション開発者がEntityクラスを用意する代わりに`TsurugiResultEntity`を使うことも出来る。

Entityクラスや`TsurugiResultEntity`への変換を定義する方法は何通りか存在する。

#### select結果取得の例（`TsurugiResultEntity`を使用する方法・カラム名指定方式）

Iceaxeが用意している`TsurugiResultEntity`を使う例。

```java
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

var sql = "select FOO, BAR, ZZZ from TEST";
try (var ps = session.createQuery(sql)) {
    tm.execute(transaction -> {
        List<TsurugiResultEntity> list = transaction.executeAndGetList(ps);
        for (var entity: list) {
            System.out.println(entity.getInt("FOO"));
            System.out.println(entity.getLongOrNull("BAR"));
            System.out.println(entity.getStringOrNull("ZZZ"));
        }
    });
}
```

`TsurugiSession`の`createQuery`メソッドで引数の`resultMapping`を省略すると、select結果のレコードが`TsurugiResultEntity`に変換されるようになる。

`TsurugiResultEntity`のgetterメソッドにカラム名を指定して値を取得する。

`TsurugiResultEntity`のgetterメソッドには以下のような種類がある。

| メソッド                          | 説明                                                         |
| --------------------------------- | ------------------------------------------------------------ |
| `getデータ型(name)`               | 値を取得する。値が`null`の場合は`NullPointerException`が発生する。 |
| `getデータ型(name, defaultValue)` | 値を取得する。値が`null`の場合は`defaultValue`が返る。       |
| `getデータ型orNull(name)`         | 値を取得する。値が`null`の場合は`null`が返る。               |
| `findデータ型(name)`              | 値を`Optional`で取得する。                                   |

#### select結果取得の例（`TsurugiResultEntity`を使用する方法・カラムの並び順依存方式）

`TsurugiResultEntity`からカラムの位置（index）を指定して値を取得する例。

```java
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;

var sql = "select ZZZ, count(*) from TEST group by ZZZ";
try (var ps = session.createQuery(sql)) {
    tm.execute(transaction -> {
        List<TsurugiResultEntity> list = transaction.executeAndGetList(ps);
        for (var entity: list) {
            System.out.println(entity.getStringOrNull(entity.getName(0)));
            System.out.println(entity.getInt(entity.getName(1)));
        }
    });
}
```

`TsurugiResultEntity`の`getName`メソッドで、指定された位置のカラム名を取得できる。

> **Note**
>
> select文で明示的にカラム名が指定されていない場合は、Iceaxeが適当な名前を生成する。

> **Note**
>
> `TsurugiResultEntity`の`getNameList`メソッドでカラム名一覧を取得できる。

#### select結果変換の例（Entityに変換する方法・カラム名指定方式）

アプリケーション開発者が用意するEntityクラス（setterメソッドを持つクラス）に変換する例。

```java
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

var sql = "select FOO, BAR, ZZZ from TEST";
var resultMapping = TgResultMapping.of(TestEntity::new)
    .addInt("FOO", TestEntity::setFoo)
    .addLong("BAR", TestEntity::setBar)
    .addString("ZZZ", TestEntity::setZzz);
try (var ps = session.createQuery(sql, resultMapping)) {
    tm.execute(transaction -> {
        List<TestEntity> list = transaction.executeAndGetList(ps);
    });
}
```

`TgResultMapping`の`of`メソッドにEntityのコンストラクターを渡し、`add`系メソッドでカラム名とsetterメソッドを追加していく。

#### select結果変換の例（Entityに変換する方法・カラムの並び順依存方式）

`TgResultMapping`の`add`系メソッドでカラム名を省略すると、カラムの並び順依存になる。

```java
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

var sql = "select ZZZ, count(*) from TEST group by ZZZ";
var resultMapping = TgResultMapping.of(TestCountByZzzEntity::new)
    .addString(TestCountByZzzEntity::setZzz)
    .addInt(TestCountByZzzEntity::setCount);
try (var ps = session.createQuery(sql, resultMapping)) {
    tm.execute(transaction -> {
        List<TestCountByZzzEntity> list = transaction.executeAndGetList(ps);
    });
}
```

#### select結果変換の例（1カラムだけ取得する場合）

1カラムだけしか取得しない場合は、`TgResultMapping`の`ofSingle`メソッドにカラムのデータ型を指定することで、その値を直接取得することが出来る。

```java
import java.util.Optional;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

var sql = "select FOO from TEST";
var resultMapping = TgResultMapping.ofSingle(int.class);
try (var ps = session.createQuery(sql, resultMapping)) {
    tm.execute(transaction -> {
        List<Integer> fooList = transaction.executeAndGetList(ps);
    });
}
```

#### select結果変換の例（`TsurugiResultRecord`から変換する方法）

`TsurugiResultRecord`クラスは、Iceaxeが1レコード分のデータを処理するクラス。（使用上の注意点については後述）

この`TsurugiResultRecord`からEntityクラスへ変換する関数を用意する例。

```java
import java.io.IOException;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

class TestEntity {
    public static TestEntity of(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        var entity = new TestEntity();
        entity.setFoo(record.getInt("FOO"));
        entity.setBar(record.getLongOrNull("BAR"));
        entity.setZzz(record.getStringOrNull("ZZZ"));
        return entity;
    }
    ～
}

var sql = "select FOO, BAR, ZZZ from TEST";
var resultMapping = TgResultMapping.of(TestEntity::of);
try (var ps = session.createQuery(sql, resultMapping)) {
    tm.execute(transaction -> {
        List<TestEntity> list = transaction.executeAndGetList(ps);
    });
}
```

`TgResultMapping`の`of`メソッドに、`TsurugiResultRecord`からEntityクラスへ変換する関数を渡す。



### `TsurugiQueryResult`

`TsurugiQueryResult`はselect結果を扱うクラス。

> **Note**
>
> `TsurugiQueryResult`で実行できる処理は、基本的に`TsurugiTransaction`の`executeAnd`系メソッドで実行できる。
>
> `executeAnd`系メソッドは`TsurugiQueryResult`を隠蔽し、クローズ処理も行うので、基本的には`TsurugiQueryResult`を操作するより`executeAnd`系メソッドの使用を推奨する。

- `TsurugiQueryResult`は`TsurugiTransaction`の`executeQuery`メソッドから返される。
  - 使用終了後にクローズする必要がある。（明示的にクローズしない場合、`TsurugiTransaction`のクローズ時にクローズされる）
- `TsurugiQueryResult`からselect結果を取得する。
  - `TsurugiQueryResult`はselect結果の複数レコードを先頭から順番に処理していく。各レコードは一度しか読むことが出来ない。
- `TsurugiQueryResult`は**スレッドセーフではない**。

`TsurugiQueryResult`からselect結果を取得するメソッドには以下のようなものがある。

| メソッド        | 説明                                                         | 代替メソッド                              |
| --------------- | ------------------------------------------------------------ | ----------------------------------------- |
| `getRecordList` | 全レコードの`List`を返す。                                   | `TsurugiTransaction.executeAndGetList`    |
| `findRecord`    | 1レコードを`Optional`で返す。                                | `TsurugiTransaction.executeAndFindRecord` |
| `iterator`      | `Iterator`を返す。（非推奨※1）                               |                                           |
| `forEach`       | レコードを処理する関数を渡す。（非推奨※1）<br />`whileEach`メソッドを推奨。 |                                           |
| `whileEach`     | レコードを処理する関数を渡す。                               | `TsurugiTransaction.executeAndForEach`    |

> **Note** ※1
>
> `TsurugiQueryResult`は`Iterable`を実装しているので`Iterable`として振る舞うことが出来るが、例外発生時は非チェック例外に変換する必要があり、非チェック例外は受け取る側で処理が漏れる危険性がある為、`Iterable`として使用することはあまり推奨しない。

その他に、`TsurugiQueryResult`から情報を取得するメソッドがある。

| メソッド        | 説明                                                         |
| --------------- | ------------------------------------------------------------ |
| `getNameList`   | カラム名一覧を返す。<br />select文でカラム名が明示されていなかった場合は、Iceaxeが適当なカラム名を付ける。 |
| `getReadCount`  | 読み込んだレコード数を返す。                                 |
| `getHasNextRow` | 次のレコードを取得する必要があるかどうか（最後のレコードまで取得したかどうか）を返す。<br />一度も取得していないと`empty`、処理中は`true`、全レコードを取得し終わったら`false`になる。 |

### `TsurugiResultRecord`

`TsurugiResultRecord`は1レコード分のデータを処理するクラス。

> **Note**
>
> `TsurugiTransaction`の`executeAnd`系メソッドや`TsurugiQueryResult`のselect結果取得系メソッドを使えば`TsurugiResultRecord`は隠蔽されるが、`TgResultMapping`で「`TsurugiResultRecord`からEntityクラスに変換する関数」を使う場合は、アプリケーション開発者が`TsurugiResultRecord`を扱う必要がある。

- `TsurugiResultRecord`は`TsurugiQueryResult`内部で生成される。
  - 生成元の`TsurugiQueryResult`がクローズされると使用できなくなる。
- select結果の1レコード分のデータを取得する為に使用する。
- `TsurugiResultRecord`は**スレッドセーフではない**。

`TsurugiResultRecord`は1レコード分のデータを処理するが、インスタンスは`TsurugiQueryResult`の中でひとつしか生成されず、複数レコード間で同じインスタンスが使い回される。
また、`TsurugiQueryResult`をクローズすると、`TsurugiResultRecord`も使用できなくなる。

したがって、アプリケーション開発者が各レコードの値を保存する目的で`TsurugiResultRecord`インスタンスを保持する（`List`に`add`していく）ような使い方は出来ない。
また、`TsurugiTransaction`の`executeAnd`系メソッドの外に`TsurugiResultRecord`インスタンスを出してはいけない。（`executeAnd`系メソッドが終了する際に`TsurugiQueryResult`がクローズされて`TsurugiResultRecord`も使用不可となる為）

そのため、`TsurugiQueryResult`からレコードを返す際は、`TsurugiResultRecord`を1レコード分の値を保持するEntityクラスへ変換する。すなわち、レコード毎にEntityインスタンスを生成して、`TsurugiResultRecord`から値を移送する。

> **Note**
>
> `TsurugiResultRecord`を1レコードずつ処理するのであれば、必ずしもEntityクラスに変換しなくてもよい。変換しない分、処理速度は向上する。
>
> ```java
> var sql = "select FOO, BAR, ZZZ from TEST";
> var resultMapping = TgResultMapping.of(record -> record); // Entityに変換しない
> try (var ps = session.createQuery(sql, resultMapping)) {
>     tm.execute(transaction -> {
>         // TsurugiResultRecordを直接使って1レコードずつ処理
>         transaction.executeAndForEach(ps, record -> {
>             int foo = record.nextInt();
>             Long bar = record.nextLongOrNull();
>             String zzz = record.nextStringOrNull();
>         });
>     });
> }
> ```

#### `TsurugiResultRecord`の使用方法

`TsurugiResultRecord`から値を取得するメソッド群は、3種類に分類される。ある群のメソッドを使用したら、他の群のメソッドは基本的に使用不可。

##### 現在カラム系

メソッド名に `CurrentColumn` が入っているメソッド群。

select文の複数カラムに対し、`moveCurrentColumnNext`メソッドによって現在カラムを移動しながら順番に値を取得していく。各カラムの値は一度しか取得できない。

select文のカラム数やデータ型に関係なく汎用的に取得する方法である。

```java
while (record.moveCurrentColumnNext()) { // カラムが存在している間ループ
    String name = record.getCurrentColumnName(); // 現在位置のカラム名取得
    Object value = record.fetchCurrentColumnValue(); // 現在位置の値取得
}
```

値を取得する`fetchCurrentColumnValue`メソッドは、`moveCurrentColumnNext`メソッドの呼び出し1回につき一度しか実行できない。

##### カラム名指定系

メソッド名が `getデータ型` あるいは `findデータ型` で始まっているメソッド群。

select文のカラム名を指定して値を取得する方法である。

```java
entity.setFoo(record.getIntOrNull("FOO"));
entity.setBar(record.getLongOrNull("BAR"));
entity.setZzz(record.getStringOrNull("ZZZ"));
```

> **Note**
>
> このメソッドを使用した場合、内部では一旦全カラムを読み込んで、カラム名をキーとする`Map`を作っている。
> 他のメソッド群ではこういった`Map`は作っていない。
> したがって、他のメソッド群と共存できない。

##### next系

メソッド名が `nextデータ型` で始まっているメソッド群。

select文の複数カラムに対し、順番に値を取得しながら現在カラムを移動していく。各カラムの値は一度しか取得できない。

`next`系メソッドを呼んだ直後に`getCurrentColumnName`（現在位置のカラム名取得）, `getCurrentColumnType`（現在位置のデータ型取得）メソッドは使用可能。

カラム名を使用せず、select文のカラムの並び順とカラム数に依存した取得方法である。

```java
entity.setFoo(record.nextIntOrNull());
entity.setBar(record.nextLongOrNull());
entity.setZzz(record.nextStringOrNull());
```



### バインド変数の使用方法（select文・更新系SQL共通）

バインド変数は、SQL文の中に変数を埋め込んでおき、SQL実行時にその変数に対してSQL文の外から値を代入する仕組みである。

バインド変数を使う場合は、SQL文とバインド変数の定義をDBサーバーに事前に登録する必要がある。

バインド変数の使い方は以下のようになる。

1. SQL文の中にコロンで始まる変数名（ `:変数名` という形式）を入れる。
   - 変数名に使える文字は、1文字目は英字またはアンダースコアで、2文字目以降は英数字またはアンダースコア。大文字小文字は区別される。
2. 変数名とそのデータ型を`TgParameterMapping`で定義する。
3. `TsurugiSession`の`createQuery`や`createStatement`メソッドにSQL文と`TgParameterMapping`を渡して`TsurugiSql`を生成する。（バインド変数の定義がDBサーバーに登録される）
4. `TsurugiTransaction`の`execute`系メソッドに`TsurugiSql`とバインド変数に代入する具体的な値（パラメーター）を渡してSQLを実行する。



バインド変数を定義・使用する方法は何通りか存在する。

#### バインド変数の例（基本形）

バインド変数の変数名を、SQL文・`TgParameterMapping`・`TgBindParameters`でそれぞれ指定する方式。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var sql = "update TEST set BAR = :bar where FOO = :foo";
var variables = TgBindVariables.of().addInt("foo").addLong("bar");
var parameterMapping = TgParameterMapping.of(variables);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = TgBindParameters.of().addInt("foo", 123).addLong("bar", 1L);
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

`TgBindVariables`は複数のバインド変数の定義を保持するクラス。
`TgBindVariables`の`add`系メソッドでバインド変数の変数名とデータ型を追加していき、`TgParameterMapping`に変換する。

`TgBindParameters`は、バインド変数に代入する値（パラメーター）を保持するクラス。
バインド変数に代入する値は、`TgBindParameters`の`add`系メソッドで追加していく。
`TgBindVariables`で指定したデータ型と同じデータ型のメソッドを使う必要がある。

#### バインド変数の例（`TgBindVariable`を使う方法）

`TgBindVariable`は、バインド変数1個の変数名とデータ型を保持するクラス。
`TgBindVariable`を使うと、バインド変数の変数名とデータ型の定義箇所を一か所だけにすることが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

// バインド変数の定義
var foo = TgBindVariable.ofInt("foo");
var bar = TgBindVariable.ofLong("bar");

var sql = "update TEST set BAR = " + bar.sqlName() + " where FOO = " + foo.sqlName();
var parameterMapping = TgParameterMapping.of(foo, bar);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = TgBindParameters.of(foo.bind(123), bar.bind(1L));
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

`TgBindVariable`の`of`系メソッドにバインド変数名を渡してインスタンスを生成する。

`TgBindVariable`の`sqlName`メソッドは `:変数名` という文字列を返す。

> **Note**
>
> `TgBindVariable`の`toString`メソッドは `:変数名/*データ型*/` というコメント付きの文字列を返すので、（コメントがネストしないなら）これをSQL文に使うことも出来る。

`TgParameterMapping`の`of`メソッドに、使用する`TgBindVariable`を全て指定する。

バインド変数に代入する値は`TgBindVariable`の`bind`メソッドで指定し、`TgBindParameters`を構築する。

> **Note**
>
> `bind`メソッドの引数は、定義時に決められたデータ型しか受け付けないので、他の型で指定してしまうようなミスを起こしにくい。

#### バインド変数の例（変数が1個だけの場合）

バインド変数を1個だけしか使用しない場合は、`TgParameterMapping`の`ofSingle`メソッドに変数名とデータ型を指定することで、パラメーターに値を直接指定することが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var sql = "update TEST set BAR = 1 where FOO = :foo";
var parameterMapping = TgParameterMapping.ofSingle("foo", int.class);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        int parameter = 123;
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

#### バインド変数の例（変数が0個の場合）

バインド変数が無いSQL文をDBサーバーに事前に登録したい場合は、`TgParameterMapping`を空にすればよい。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var sql = "update TEST set BAR = 1 where FOO = 123";
var parameterMapping = TgParameterMapping.of();
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = TgBindParameters.of();
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

パラメーターには`TgBindParameters.of()`または`null`を指定する。



#### バインド変数の例（Entityを変換する方法）

アプリケーション開発者が用意するEntityクラス（getterメソッドを持つクラス）に対し、各getterメソッドで取得した値をバインド変数に割り当てることが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var sql = "insert into TEST (FOO, BAR, ZZZ) values(:foo, :bar, :zzz)";
var parameterMapping = TgParameterMapping.of(TestEntity.class)
    .addInt("foo", TestEntity::getFoo)
    .addLong("bar", TestEntity::getBar)
    .addString("zzz", TestEntity::getZzz);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = new TestEntity(123, 456L, "abc");
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

`TgParameterMapping`の`of`メソッドに、パラメーターとして使いたいクラスを指定する。その後、`add`系メソッドでバインド変数名とgetterメソッドを追加していく。

実行時のパラメーターには、`TgParameterMapping`の`of`メソッドで指定したクラスのオブジェクトが直接指定できる。

#### バインド変数の例（`TgBindVariable`を使ってEntityを変換する方法）

`TgBindVariable`を使ってEntityクラスの変換定義を記述することが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var foo = TgBindVariable.ofInt("foo");
var bar = TgBindVariable.ofLong("bar");
var zzz = TgBindVariable.ofString("zzz");

var sql = "insert into TEST (FOO, BAR, ZZZ)"
    + " values(" + TgBindVariables.toSqlNames(foo, bar, zzz) + ")";
var parameterMapping = TgParameterMapping.of(TestEntity.class)
    .add(foo, TestEntity::getFoo)
    .add(bar, TestEntity::getBar)
    .add(zzz, TestEntity::getZzz);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = new TestEntity(123, 456L, "abc");
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

`TgParameterMapping`の`add`メソッドを使い、第1引数に`TgBindVariable`を渡す。

`TgBindVariables`の`toSqlNames`メソッドは、`:変数名`形式のバインド変数名をカンマ区切りで結合した文字列を返す。

#### バインド変数の例（`TgBindVariables`と変換関数を用意してEntityを変換する方法）

「Entityクラスから`TgBindParameters`に変換する関数」を`TgParameterMapping`に登録しておく方式。
変換関数を用意するので、関数内に複雑な処理を記述することが出来る。

```java
import java.util.function.Function;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var variables = TgBindVariables.of().addInt("foo").addLong("bar").addString("zzz");
var sql = "insert into TEST (FOO, BAR, ZZZ) values(" + variables.getSqlNames() + ")";
Function<TestEntity, TgBindParameters> parameterConverter = entity -> {
    String zzz = entity.getZzz();
    if (zzz == null) {
        zzz = "default";
    }
    return TgBindParameters.of()
        .add("foo", entity.getFoo())
        .add("bar", entity.getBar())
        .add("zzz", zzz);
};
var parameterMapping = TgParameterMapping.of(variables, parameterConverter);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = new TestEntity(123, 456L, "abc");
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

変換関数の他に、`TgBindVariables`（バインド変数の定義）は必要。

`TgBindVariables`の`getSqlNames`メソッドは、`:変数名`形式のバインド変数名をカンマ区切りで結合した文字列を返す。



## その他の機能

### テーブルメタデータの取得

テーブルメタデータ（テーブルの定義情報）を取得することが出来る。

```java
import java.util.Optional;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;

Optional<TgTableMetadata> metadata = session.findTableMetadata("TEST");
```

`TsurugiSession`の`findTableMetadata`メソッドにテーブル名を指定する。
返ってくるのは`Optional`であり、テーブルが存在しない場合はemptyになる。すなわち、テーブルの存在確認に利用できる。

> **Warning**
>
> `TgTableMetadata`にはデータベース名やスキーマ名を取得するメソッドがあるが、現在は未実装。

> **Warning**
>
> `TgTableMetadata`からカラム一覧を取得することが出来るが、現時点では情報量は少ない（データ型の詳細やキー情報・インデックス情報等は取得できない）。



### SQLの実行計画の取得

SQLの実行計画を取得することが出来る。

```java
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

var foo = TgBindVariable.ofInt("foo");
var sql = "select * from TEST where foo=" + foo.sqlName();
var parameterMapping = TgParameterMapping.of(foo);

try (var ps = session.createQuery(sql, parameterMapping)) {
    var parameter = TgBindParameters.of(foo.bind(123));
    TgStatementMetadata statementMetadata = ps.explain(parameter);
    PlanGraph planGraph = statementMetadata.getLowPlanGraph();
    System.out.println(planGraph.toString());
}
```

`TsurugiSql`の`explain`メソッドでSQLのメタデータを取得する。

バインド変数がある場合は、`explain`メソッドの引数にSQLのパラメーター（SQLを実行するときに渡すパラメーターと同じ）を渡す。

実行計画を取得する例は、[iceaxe-examples]の`Example61Explain`を参照。

> **Warning**
>
> `TsurugiSqlQuery`と`TsurugiSqlStatement`（SQL文をDBサーバーに事前に登録しない方式）では、実行計画取得は未実装。UnsupportedOperationExceptionが発生する。



### タイムアウト時間の設定

DBサーバーと通信する箇所では、タイムアウト時間を指定できる。

> **Note**
>
> 実際には、デフォルトのタイムアウト時間と、コミットのタイムアウト時間くらいしか指定することは無いと思われる。
>
> （コミットは、トランザクションの大きさによっては、かなりの時間がかかることがある）

タイムアウト時間を指定する例は[iceaxe-examples]の`Example92Timeout`を参照。

#### `TgSessionOption`によるタイムアウト指定

タイムアウト時間は、`TsurugiSession`生成時に指定する`TgSessionOption`で指定できる。

どの箇所のタイムアウト時間かは、`TgTimeoutKey`列挙型で指定する。
`TgTimeoutKey.DEFAULT`で、デフォルトの（全ての箇所で共通の）タイムアウト時間を指定できる。デフォルトのタイムアウト時間を指定しなかった場合は、`Long.MAX_VALUE`[ナノ秒]になる（実質、無限待ち）。

##### `TgSessionOption`でタイムアウト時間を指定する例

```java
import java.util.concurrent.TimeUnit;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;

var sessionOption = TgSessionOption.of();
sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.MINUTES);
sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.HOURS);

try (var session = connector.createSession(sessionOption)) {
    ～
}
```

#### 個別のタイムアウト指定

DBサーバーとの通信を伴うクラスでは、タイムアウト時間を指定するメソッドがある。

##### `TsurugiTransaction`でタイムアウト時間を指定する例

```java
import java.util.concurrent.TimeUnit;

tm.execute(transaction -> {
    transaction.setCommitTimeout(1, TimeUnit.HOURS);
    ～
});
```

#### `TsurugiTransactionManager`によるタイムアウト指定

`TsurugiTransactionManager`で生成する`TsurugiTransaction`に対しては、`TgTmSetting`でタイムアウト時間を指定することが出来る。

##### `TsurugiTransactionManager`でタイムアウト時間を指定する例

```java
import java.util.concurrent.TimeUnit;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var setting = TgTmSetting.of(TgTxOption.ofOCC()).commitTimeout(1, TimeUnit.HOURS);
var tm = session.createTransactionManager(setting);
tm.execute(transaction -> {
    ～
});
```



### Entityにおける列挙型の使用

select結果を保持したりバインド変数に変換したりする為にアプリケーション開発者が用意するEntityクラスにおいて、テーブルではvarcharやint等で扱っているデータを、Entityでは列挙型で保持する場合の変換方法について。

#### EntityにDBのデータ型に変換するメソッドを用意する方法

一番分かりやすいのは、Entityクラスにおいて、DBで保存するデータ型で取得・設定できるメソッドを用意する方法である。

```java
class ExampleEntity {
    private ExampleType exampleType; // 列挙型

    public void setExampleType(ExampleType type) {
        this.exampleType = type;
    }

    public ExampleType getExampleType() {
        return this.exampleType;
    }

    public void setExampleTypeAsString(String type) {
        this.exampleType = ExampleType.valueOf(type);
    }

    public String getExampleTypeAsString() {
        return exampleType.name();
    }
}

var parameterMapping = TgParameterMapping.of(ExampleEntity.class)
    .addString("EXAMPLE_TYPE", ExampleEntity::getExampleTypeAsString);
var resultMapping = TgResultMapping.of(ExampleEntity::new)
    .addString("EXAMPLE_TYPE", ExampleEntity::setExampleTypeAsString);
```

#### `TgParameterMapping`・`TgResultMapping`に変換関数を渡す方法

`TgParameterMapping`や`TgResultMapping`クラスの`add`系メソッドにおいて、Entityのgetter/setterメソッドの他に、値を変換する関数を渡すことが出来る。

```java
class ExampleEntity {
    private ExampleType exampleType; // 列挙型

    public void setExampleType(ExampleType type) {
        this.exampleType = type;
    }

    public ExampleType getExampleType() {
        return this.exampleType;
    }
}

var parameterMapping = TgParameterMapping.of(ExampleEntity.class)
    .addString("EXAMPLE_TYPE", ExampleEntity::getExampleType, ExampleType::name);
var resultMapping = TgResultMapping.of(ExampleEntity::new)
    .addString("EXAMPLE_TYPE", ExampleEntity::setExampleType, ExampleType::valueOf);
```

`add`系メソッドは、DBで保存するデータ型のメソッドを使用する。

`TgParameterMapping`では、第3引数にEntityのデータ型（列挙型）からテーブルのデータ型へ変換する関数を渡す。
`TgResultMapping`では、第3引数にテーブルのデータ型からEntityのデータ型（列挙型）へ変換する関数を渡す。

#### `TgBindVariable`を拡張する方法

バインド変数に関しては、`TgBindVariable`を継承して独自のクラスを作り、それを使ってEntityから変換することが出来る。

```java
class ExampleTypeBindVariable extends TgBindVariable<ExampleType> {

    public ExampleTypeBindVariable(String name) {
        super(name, TgDataType.STRING); // テーブルのデータ型
    }

    @Override
    public TgBindParameter bind(ExampleType value) {
        String v = (value != null) ? value.name() : null; // テーブルのデータ型へ変換
        return TgBindParameter.of(name(), v);
    }

    @Override
    public TgBindVariable<ExampleType> clone(String name) {
        return new ExampleTypeBindVariable(name);
    }
}

var type = new ExampleTypeBindVariable("type");
var sql = "insert into EXAMPLE(EXAMPLE_TYPE) values(" + type.sqlName() + ")";
var parameterMapping = TgParameterMapping.of(ExampleEntity.class)
    .add(type, ExampleEntity::getExampletype);
```

`TgParameterMapping`の`add`メソッドに`TgBindVariable`を渡す方法を用いる。



### 例外ハンドリング

DBサーバーとの通信中に発生した`IOException`および`InterruptedException`は、そのままスローされる。

トランザクションの実行中（SQLの実行やコミットの実行等）にDBサーバーからエラーが返された場合は、`TsurugiTransaction`から`TsurugiTransactionException`がスローされる。
トランザクションの実行以外でDBサーバーからエラーが返された場合は、`TsurugiIOException`がスローされる。

`TsurugiTransactionException`や`TsurugiIOException`の`getDiagnosticCode`メソッドで、DBサーバーから返されたエラーコードを取得できる。
エラーコードは、主に`SqlServiceCode`列挙型の値である。

> **Warning**
>
> 現在のエラーコードは未整理なので、今後変わる可能性がある。

`TsurugiTransactionManager`は、`TsurugiTransactionException`から取得したエラーコードを用いてシリアライゼーションエラー（リトライ可能なアボート）かどうかを判定する。

シリアライゼーションエラーだった場合に`TsurugiTransactionManager`はトランザクションの再実行を行うが、実行回数が指定回数を超えた場合は、`TsurugiTransactionManager`の`execute`系メソッドから`TsurugiTmRetryOverIOException`（リトライ回数オーバーの例外）がスローされる。

`TsurugiTransaction`で発生した`TsurugiTransactionException`がシリアライゼーションエラーでない場合は、`TsurugiTransactionManager`の`execute`系メソッドからは`TsurugiTmIOException`がスローされる。

> **Note**
>
> `TsurugiTransaction`で発生した例外は、`TsurugiTransactionManager`からは`IOException`としてスローされる。

#### エラーコードを取得する例

```java
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public boolean isRetryable(TsurugiTransaction e) {
    var code = e.getDiagnosticCode();
    if (code == SqlServiceCode.ERR_SERIALIZATION_FAILURE) {
        // シリアライゼーションエラー（リトライ可能なアボート）
        return true;
    }
    return false;
}
```

#### `TsurugiTransactionException`から取得できる情報

`TsurugiTransactionException`からはエラーコード以外に以下のような情報（エラー発生時の値）を取得できる。
（例外の発生個所によっては取得できない場合がある。その場合は`null`や`0`が返る）

##### SQL実行時に発生した場合の情報

| メソッド                | 情報                                                         | 情報元               |
| ----------------------- | ------------------------------------------------------------ | -------------------- |
| `getIceaxeTxId`         | Iceaxeが付けたトランザクション番号                           | `TsurugiTransaction` |
| `getIceaxeTmExecuteId`  | `TsurugiTransactionManager`の`execute`メソッドを実行する際に付けられた番号 | `TsurugiTransaction` |
| `getAttempt`            | `TsurugiTransactionManager`での試行番号                      | `TsurugiTransaction` |
| `getTransactionOption`  | トランザクションオプション                                   | `TsurugiTransaction` |
| `getTransactionId`      | DBサーバー側が採番したトランザクションID                     | `TsurugiTransaction` |
| `getSqlStatement`       | `TsurugiSql`                                                 | `TsurugiSql`         |
| `getSqlParameter`       | SQL実行時のバインド変数の値（パラメーター）                  |                      |
| `getIceaxeSqlExecuteId` | SQL実行毎にIceaxeで採番された番号                            | `TsurugiSqlResult`   |

##### `TsurugiTransaction`のメソッドで発生した場合の情報

| メソッド               | 情報                                                         | 情報元               |
| ---------------------- | ------------------------------------------------------------ | -------------------- |
| `getTxMethod`          | 発生個所のメソッド名を表す列挙型                             | `TsurugiTransaction` |
| `getIceaxeTxExecuteId` | `TsurugiTransaction`の`execute`系メソッドを実行する際に付けられた番号 | `TsurugiTransaction` |



### トランザクション再実行判定処理のカスタマイズ

`TgTmTxOptionSupplier`（`TsurugiTransactionManager`で`TsurugiTransaction`を生成する際の`TgTxOption`を提供するクラス）は、これを継承して独自のクラスを作ることが出来る。

```java
class MyTxOptionSupplier extends TgTmTxOptionSupplier {

    @Override
    public Object createExecuteInfo(int iceaxeTmExecuteId) {
        return new MyExecuteInfo();
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        var info = (MyExecuteInfo)executeInfo;
        return TgTmTxOption.execute(TgTxOption.ofOCC());
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException e, TgTmRetryInstruction retryInstruction) {
        var info = (MyExecuteInfo)executeInfo;
        return TgTmTxOption.execute(TgTxOption.ofLTX("table1", "table2"));
    }

    @Override
    protected TgTmRetryInstruction isRetryable(TsurugiTransaction transaction, TsurugiTransactionException e) {
        return super.isRetryable(transaction, e);
    }
}

var setting = TgTmSetting.of(new MyTxOptionSupplier());
```

`isRetryable`メソッドで、再実行可能かどうか（発生した例外がシリアライゼーションエラー（リトライ可能なアボート）かどうか）を判定する。

- `isRetryable`メソッドは`TgTmRetryInstruction`を返す。これは、どのような種類の再実行が可能なのかと、その理由となるメッセージを保持している。
  - 例えば、通常の再実行可能と、OCCが他LTXのwrite preserveの範囲だった場合の再実行可能の区別が出来る。
- `isRetryable`メソッドのデフォルトでは、`TgTmTxOptionSupplier`の`setRetryPredicate`メソッドでセットされた判定関数が呼ばれる。
  - 判定関数を明示的にセットしていない場合は、`TsurugiDefaultRetryPredicate`のシングルトンインスタンスが使われる。このシングルトンインスタンスは変更可能なので、Iceaxe全体で再実行判定を変更したい場合は、ここを差し替えればよい。

`computeFirstTmOption`メソッドで初回実行用の`TgTmTxOption`を返し、`computeRetryTmOption`メソッドで再実行用の`TgTmTxOption`を返す。
`computeRetryTmOption`メソッドは、`isRetryable`メソッドによって再実行可能と判定されたときのみ呼ばれる。

`TgTmTxOption`は`TgTmTxOptionSupplier`からトランザクションオプションを返す為に使用するクラスで、以下のような生成メソッドを持つ。

| 生成メソッド                          | 説明                                                         |
| ------------------------------------- | ------------------------------------------------------------ |
| `execute(txOption, retryInstruction)` | トランザクションが実行可能な場合に、使用する`TgTxOption`を保持する。 |
| `retryOver(retryInstruction)`         | 再実行回数を超えた（リトライ回数オーバーになった）ことを表す。 |
| `notRetryable(retryInstruction)`      | トランザクションが再実行できない（シリアライゼーションエラーでない例外が発生した）ことを表す。 |

トランザクション再実行判定の間で共有したいデータがある場合は、`createExecuteInfo`メソッドをオーバーライドし、アプリケーション開発者が共有したいオブジェクトを生成して返す。共有したいデータが無い場合は`null`を返せばよい。

- `createExecuteInfo`メソッドは、`TsurugiTransactionManager`の`execute`メソッドが呼ばれる毎に1回だけ呼ばれる。
- `createExecuteInfo`メソッドから返されたオブジェクトは`computeFirstTmOption`や`computeRetryTmOption`メソッドに渡される。
  - 渡される共有データの型は`Object`なので、アプリケーション開発者が使用する型にキャストする必要がある。



### イベントリスナー

Iceaxeで処理を行うクラス（`TsurugiConnector`や`TsurugiSession`・`TsurugiTransaction`・`TsurugiTransactionManager`、`TsurugiSql`の具象クラス等）では、処理を行った際に呼ばれるイベントリスナー（コールバック関数）を登録することが出来る。
イベントリスナーは、基本的に、対象クラス名の末尾に `EventListener` を付加した名前のインターフェースである。（Javaが標準で提供している関数型インターフェースを使っている箇所もある）

処理件数をカウントしたり、ログを出力したり、インスタンスが生成された時にそれに対して初期設定を追加したりするのに利用できる。

イベントリスナーは、対象クラスの`addEventListener`メソッドで登録する。

```java
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;

// TsurugiTransactionManagerでリトライされたときにログを出力する例
var tm = session.createTransactionManager(setting);
tm.addEventListener(new TsurugiTmEventListener() {
    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextTxOption) {
        LOG.info("transaction retry: {}", cause.getMessage());
    }
});
```

イベントリスナーを使って実装されている例として、以下のようなクラスがある。

| 内容                                                        | クラス名                                    | イベントリスナー              |
| ----------------------------------------------------------- | ------------------------------------------- | ----------------------------- |
| `TsurugiTransactionManager`の各処理の実行件数をカウントする | `TgTmSimpleCounter`<br />`TgTmLabelCounter` | `TsurugiTmEventListener`      |
| トランザクションに関する各処理のログを出力する              | `TsurugiSessionTxFileLogger`                | `TsurugiSessionEventListener` |



### `IceaxeConvertUtil`

`TgParameterMapping`や`TgResultMapping`（`TsurugiResultRecord`や`TsurugiResultEntity`）において、「アプリケーション開発者が使用するデータ型」と「通信で使われるデータ型」との変換を行う際に使われるクラスが`IceaxeConvertUtil`。

独自の変換を行いたい場合は、`IceaxeConvertUtil`を継承したクラスを作り、`TgParameterMapping`や`TgResultMapping`に登録することが出来る。

```java
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

static final IceaxeConvertUtil MY_CONVERT_UTIL = new IceaxeConvertUtil() {

    @Override
    protected String convertString(Object obj) {
        if (obj instanceof Double) {
            return String.format("%.2f", obj); // 独自の変換ルール
        }
        return super.convertString(obj);
    }
}

var parameterMapping = TgParameterMapping.of(TestEntity.class)
    ～
    .setConvertUtil(MY_CONVERT_UTIL);
var resultMapping = TgResultMapping.of(TestEntity::new)
    ～
    .setConvertUtil(MY_CONVERT_UTIL);
```

`IceaxeConvertUtil`の例は、[iceaxe-examples]の`Example94TypeConvert`を参照。



## 制限事項

2023-06-01時点でのIceaxeの制限事項。

- `TsurugiTransaction`の`executeAndGetCount`メソッドや`TsurugiStatementResult`の`getUpdateCount`メソッドの戻り値は、更新系SQLで処理された件数を返す想定だが、現時点ではDBサーバー側にその実装が無い為、常に-1を返す。
  - `executeAndGetCount`メソッドや`getUpdateCount`メソッド内で、更新系SQLの処理が完了したかどうかの確認は行われる。

以下の制限事項はIceaxeに起因するものではないが、Iceaxeの使用時に影響がある事項なので、ここに記載する。

- 対応していないSQL構文やデータ型がある。
  - 未対応の機能を使うと、エラーになる場合と異なる挙動をする場合がある。
    - like演算子は機能しない。
    - inner join以外のjoinはinner joinとして動作する。
  - テーブル名やカラム名の大文字小文字は区別される。
  - char・varcharに指定する桁数は、バイト数である。
    - 文字列はUTF-8に変換されてDBに格納される。
- DDLとDMLを同一トランザクション内で実行した場合や、別トランザクションでも並行に実行した場合は動作保証外。
  - DBサーバーの応答が無くなったりクラッシュしたりすることがある。
- トランザクションオプションのpriority, read areaは未実装。
  - 指定することは出来るが、効果は無い。
- コミットオプションは未実装。
  - 何かを指定する必要はあるが、効果は無い。
- `TsurugiTransaction`の`executeDdl`メソッド・SQL文をDBサーバーに事前に登録しない方式の`executeQuery`, `executeStatement`メソッドを実行して、DBサーバー側で文法エラー等のエラーが発生した際に、エラーメッセージに詳細情報が含まれない。
  - 「error in db_->create_executable()」というエラーメッセージが返る。（SQL文をDBサーバーに事前に登録する方式の場合は詳細なエラーメッセージが返る）
- `TsurugiSqlQuery`と`TsurugiSqlStatement`（SQL文をDBサーバーに事前に登録しない方式の`TsurugiSql`）では、実行計画の取得処理（`explain`メソッド）は未実装。
  - 使用すると、Tsubakuroで`UnsupportedOperationException`が発生する。
- コミット処理中にタイムアウト等の例外が発生してロールバックを呼び出しても、ロールバックされない。
  - Tsubakuroでは、コミットを開始した後にロールバックを呼んでも、何もせずに正常終了を返す。
- クライアント（Tsubakuroを使用するアプリケーション）を強制終了すると、DBサーバー側はクリーンアップされないので、トランザクション等が残り続ける。

