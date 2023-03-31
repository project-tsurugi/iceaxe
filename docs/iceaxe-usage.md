# Iceaxe使用方法（2023-03-31）

Iceaxeの使用方法（概要）を説明する。



## はじめに

『Iceaxe（アイスアックス）』は、Java（Java11以降）でTsurugiのDBに対してSQLを実行するのに使用するライブラリー（API）。

JavaでTsurugiのDBにアクセスするライブラリー（API）には『Tsubakuro』というものもあり、Iceaxeは内部でTsubakuroを使用している。
Iceaxeはユーザープログラマーの利便性を高める為に用意されている。
このため、Iceaxeを高レベルAPI、Tsubakuroを低レベルAPIと呼ぶことがある。

IceaxeはJDBCではないが、その位置付け（プログラムのレイヤー）はJDBCと同様である。
（ORMのような高度な機能は持たない。（ユーザープログラマーが用意したEntityクラスからSQL文を生成したり、SQL文からEntityクラスを生成したりするような機能は対象外））



## IceaxeがJDBCでない理由

Iceaxe（およびTsubakuro）はJDBCではない。

主な理由は、Tsurugiのトランザクションでは開始時やコミット時にオプションを指定する必要があるが、JDBCではその手段が無い為である。

また、SQLのバインド変数（プレースホルダー）の指定方法も異なる。（プレースホルダーはJDBCでは `?` だが、Tsurugiでは `:変数名` という形式で表す。また、Tsurugiではバインド変数を定義するときにデータ型も指定する必要がある）



## IceaxeとTsubakuroの関係

JavaでTsurugiのDBにアクセスするにはTsubakuroだけあればよい。
Iceaxeも内部ではTsubakuroを呼び出しているだけである。

しかし、IceaxeはTsubakuroをラップして（包み隠して）、ユーザープログラマーの利便性を高める機能を提供している。

IceaxeとTsubakuroの主な違いは以下のようなものである。

| 観点      | Iceaxe                                                       | Tsubakuro                                                    |
| --------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 機能      | SQLを実行する機能が対象。                                    | 全ての機能が対象。（SQLを実行する機能だけでなく、例えば、バックアップを行うDataStoreがある） |
| API方式   | 同期APIである。すなわち、処理を行うメソッドを呼び出すと、処理が終わるまで制御が返ってこない。 | 非同期APIである。すなわち、処理を行うメソッドを呼び出すとFutureが返ってくる。 |
| リトライ  | トランザクションがシリアライゼーションエラー（リトライ可能なアボート）になった時に再実行する機能を提供している。 | トランザクションの再実行に関する機能は無い。                 |
| ResultSet | select文の結果のデータ型をユーザープログラマーが利用したい型に変換して取得する機能がある。カラム名を指定して取得することも可能。 | select文の結果を取得する際は、返ってきたデータ型で取得する必要がある。また、カラム名を指定することは出来ず、カラムの並び順に従って取得する。各カラムのデータは一度だけしか取得できない。 |

IceaxeはTsubakuroをラップしているので、Iceaxeを使用する場合は、ユーザープログラムには基本的にTsubakuroのクラスは現れない。
ただし、一部のクラスや列挙型（認証情報のCredentialクラスやエラーコード等）では、Tsubakuroのクラスをユーザープログラマーが直接使用することもある。

Iceaxeが提供しているクラスの名前には、基本的に`Tsurugi` という接頭辞が付けられている。
ただし、ユーザープログラマーが直接使用する頻度が高いクラス（ユーザープログラマーが直接インスタンスを生成するクラスや列挙型等）については、クラス名を短くする為に`Tg`という接頭辞になっている。
結果として、概ね、処理を行うのが主目的のクラスは `Tsurugi` 、値を保持するのが主目的のクラスは `Tg` となっている。
（一部のクラスは `Iceaxe` という接頭辞だが、これらは基本的にIceaxe内部で使用する目的のものである）Tsubakuroが提供しているクラスにはこういった命名規則は無い。

ほとんどのIceaxeのクラスは、Tsubakuroのメソッドを呼び出し、返ってきたFutureを内部で保持する。
必要になった時点でFutureから値を取り出して使用する。（いわゆる遅延評価）

Tsubakuroはデータ受信の為にセッション毎にスレッドを作成するが、Iceaxeはスレッドを作成しない。



## IceaxeのAPI（クラス・メソッド）

### 使用手順の概略

Iceaxeを用いてTsurugiのDBでSQLを実行する手順は、概ね以下のようになる。

1. Tsurugi DBの場所と接続方法・認証情報を指定して『コネクター』（TsurugiConnectorクラス）を生成する。
2. コネクターから『セッション』（TsurugiSessionクラス）を生成する（DBに接続する）。
3. セッションからSQLを表す『SQLステートメント』（TsurugiSqlのサブクラス）を生成する。
4. セッションから『トランザクションマネージャー』（TsurugiTransactionManagerクラス）を生成する。
5. トランザクションマネージャーの内部で『トランザクション』（TsurugiTransactionクラス）を生成する。
6. トランザクションを用いてSQLステートメントを実行する。
7. トランザクションをコミット（またはロールバック）し、クローズする。
8. 使い終わったSQLステートメントをクローズする。
9. セッションをクローズする。

※SQLステートメントとトランザクションを生成する順序は任意（トランザクションの途中でSQLステートメントを生成・クローズしても問題ない）



### コネクター

Tsurugi DBの場所と接続方法は『エンドポイント』と呼ばれるURIで指定する。
TsurugiConnectorクラスでエンドポイントを（必要に応じて認証情報も）保持する。

TsurugiConnectorはセッションを生成するのに使用する。

TsurugiConnectorはセッション生成に関してスレッドセーフ。

#### エンドポイントについて

エンドポイントの内容はTsubakuroの仕様に準ずる。現時点では以下の通り。

Tsurugi DBへの接続方法には、TCP接続とIPC接続がある。
TCP接続は、データ通信にTCP/IP（ソケット通信）を用いる方法である。ただし、現時点のTCP接続は暫定実装という位置付けであり、性能はあまり良くない。
IPC接続は、データ通信にLinuxのプロセス間通信（共有メモリー）を用いる方法である。このため、Tsurugi DBと同一マシン上でないと使用できないが、最も高速である。

TCP接続の場合は、URIのスキーマ名をtcpとし、Tsurugi DBのIPアドレス（ホスト名）とTCPポート番号を指定する。
例：`tcp://localhost:12345`

IPC接続の場合は、URIのスキーマ名をipcとし、Tsurugi DBのデータベース名を指定する。
例：`ipc:tateyama`

これらのエンドポイントの値は、Tsurugi DBを起動する際に読み込まれる設定ファイルの `stream_endpoint` や `ipc_endpoint` に記述されている。

#### コネクターを生成する例

TsurugiConnectorインスタンスはTsurugiConnectorクラスのofメソッドで生成する。

```java
import java.net.URI;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

var endpoint = URI.create("tcp://localhost:12345");
var credential = new UsernamePasswordCredential("user", "password");
var connector = TsurugiConnector.of(endpoint, credentail);
```

エンドポイントの他に、セッションを生成するのに必要な認証情報を指定することが出来る。（ここで指定せず、セッションを生成するcreateSessionメソッドの引数として渡すことも出来る）
認証情報のCredentialクラスは、Tsubakuroのものを使用する。

TsurugiConnectorを生成する例は、iceaxe-examplesのExample01Connectorを参照。
Credentialの例は、Example01Credentialを参照。

制限事項：現時点では認証は実装されていないので、どんなCredentialを指定しても良い。



### セッション

TsurugiSessionクラスで、Tsurugi DBに接続して通信（データ送受信）を行う。

例えばTCP接続の場合、ひとつのセッションインスタンスが（Tsubakuro内で）ひとつのTCPソケットを保持する。

セッションからは複数のSQLステートメントやトランザクションが同時に生成できる（マルチスレッドで生成可能）。

SQLステートメントやトランザクションは、DBとの通信に（生成元の）セッションを使用する。
このため、（SQLステートメントやトランザクションの操作自体は並列に実行可能なのだが）実際の通信を行うセッションでは（Tsubakuro内では）通信データをキューに入れ、順番に送信する。
したがって、ひとつのセッションから大量にトランザクションを生成してSQLを並列に実行すると、キューが詰まって実行が遅くなる可能性がある。

同時に生成できるセッション数にはDB側に上限がある。

TsurugiSessionのcreate系メソッド（SQLステートメントの生成やトランザクションの生成等）はスレッドセーフ。

#### セッションを生成する例

TsurugiSessionインスタンスはTsurugiConnectorのcreateSessionメソッドで生成する。
セッションインスタンスを使い終わったら、**必ず**クローズする必要がある。

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

TgSessionOptionには、通信のタイムアウト時間や“トランザクションのコミット時に指定するコミットオプション”のデフォルト値を設定することが出来る。

TgSessionOptionを指定する必要がない場合は、引数なしのcreateSessionメソッドを利用できる。

```java
try (var session = connector.createSession()) {
    ～
}
```

TgSessionOptionやTsurugiSessionを生成する例は、iceaxe-examplesのExample02Sessionを参照。



### トランザクション

TsurugiTransactionは、1回分のトランザクションを管理するクラス。
SQLを実行する際にトランザクションを使用する。

TsurugiTransactionインスタンスは、TsurugiSessionのcreateTransactionメソッドによって生成する。
トランザクションは生成と同時に開始され、コミットまたはロールバックしてから、クローズして終了する。

トランザクションの開始時には『トランザクションオプション』を指定する。
コミット時には『コミットオプション』を指定する。

TsurugiTransactionの状態取得メソッドやexecute系メソッド（SQLステートメントの実行）・コミット・ロールバックはスレッドセーフ。

コミット・ロールバックは一度しか実行できない。
コミット完了後にコミットやロールバックを呼んでも無視される。ロールバック完了後にロールバックを呼んでも無視される。ロールバック完了後にコミットを呼ぶと例外が発生する。（コミット完了後にロールバックを呼んでも無視するのは、try文の本体でコミットしてfinally節でロールバックを呼んでも問題ないようにする為）
制限事項：現時点のTsubakuroでは、コミットやロールバック処理中に例外が発生した場合、その後にコミットやロールバックを呼んでも無視される。

コミットやロールバック完了後にSQLを実行しようとすると、（Tsubakuroで）例外が発生する。

トランザクションの実行中にDB側の要因によって例外（例えばinsertの一意制約違反）が発生すると、そのトランザクションはそれ以上使用できなくなる。（トランザクションがinactiveになる）

なお、Iceaxeの基本的な使用方法としては、トランザクションインスタンスのライフサイクル（生成・コミット・ロールバック・クローズ）は後述のトランザクションマネージャーで管理することを想定している。

#### トランザクションオプション

Tsurugiでは、トランザクション開始時にトランザクションオプション（OCC・LTX・RTXのいずれか）を指定する。

トランザクションオプションによって、どの時点のデータが読まれるかや、2つのトランザクションの処理が競合したときにどちらがシリアライゼーションエラー（リトライ可能なアボート）になるかが違ってくる。

- OCC
  - 実行時間が短いトランザクション。
    - TsubakuroのTransactionType.SHORTに該当する。
    - 処理時間が20～40ミリ秒程度（DBの1 epochの長さ未満）で処理件数が少量（数百件程度）であれば、LTXより高速。
  - LTXと競合した場合、特殊な場合を除いてOCCがシリアライゼーションエラーになる。
  - OCC同士が競合した場合、後からコミットしようとした方がシリアライゼーションエラーになる。
- LTX
  - 実行時間が長いトランザクションのうち、データの更新（insert/update/delete）を伴うもの。
    - TsubakuroのTransactionType.LONGに該当する。
  - 更新対象テーブルを全てwrite preserveに指定する必要がある。
    - write preserveに指定したテーブルを更新しなくても構わないが、余計なテーブルを指定していると、DB側で余分な競合チェックが働くことになる。
  - OCCと競合した場合、特殊な場合を除いてOCCがシリアライゼーションエラーになる。
  - LTX同士が競合した場合、後からトランザクション実行を開始した方がシリアライゼーションエラーになる。
- RTX
  - 実行時間が長いトランザクションのうち、クエリー（select）のみのもの。
    - TsubakuroのTransactionType.READ_ONLYに該当する。
  - 他のトランザクションと競合しない。
  - 基本的にRTX開始時点のデータが読まれる。ただし、先に開始しているLTXがあった場合、そのLTX開始前のデータが読まれる。
    - 先に開始したLTXの開始時点で他に実行中のLTXがあったら、最悪の場合はそれら全ての開始前になるので、かなり古いデータが読まれることもある。

Iceaxeでは、トランザクションオプションはTgTxOptionクラスで表す。

TgTxOptionはスレッドセーフ。

```java
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var occ = TgTxOption.ofOCC();
var ltx = TgTxOption.ofLTX("table1", "table2"); // write preserve table
var rtx = TgTxOption.ofRTX();
```

##### トランザクションオプションに指定できるパラメーター

- OCC・LTX・RTX共通
  - label
    - トランザクションのラベル（人間が見る為のもの）
- LTX・RTX共通
  - priority
    - トランザクションの優先度（排他的実行の制御）
    - TsubakuroのTransactionPriority列挙型で指定する。
      - TRANSACTION_PRIORITY_UNSPECIFIED（デフォルト）
        - EXCLUDEのトランザクションが実行中だったら、実行しない（自分がfailする）。
      - INTERRUPT
        - 実行中の他トランザクションをfailさせてから実行する。ただし、EXCLUDEのトランザクションが実行中だったら、実行しない（自分がfailする）。
      - WAIT
        - 実行中のトランザクションがあったら、それら全てが終わるのを待ってから実行する。
        - 待っている間、新しいトランザクションは実行できない（failさせる）。
      - EXCLUDE有無
        - EXCLUDEなし
          - 自分の実行中に新しいトランザクションを実行可能。
        - EXCLUDEあり
          - 自分の実行中に新しいトランザクションは実行できない（failさせる）。
    - 制限事項：priorityは未実装。指定は出来るが、効果は無い。
  - incusive read area
    - 参照する（select/insert/update/deleteを実行する対象の）テーブル名
    - 制限事項：inclusive read areaは未実装。指定は出来るが、効果は無い。
  - exclusive read area
    - 参照しない（select/insert/update/deleteを実行しない対象の）テーブル名
    - 制限事項：exclusive read areaは未実装。指定は出来るが、効果は無い。
- LTX
  - write preserve
    - 更新対象のテーブル名

TgTxOptionを生成する例は、iceaxe-examplesのExample03TxOptionを参照。

#### コミットについて

Tsurugiでは（Tsurugi以外でもトランザクション分離レベルがSERIALIZABLEであるRDBMSでは）、コミットが成功するまで、データに信頼性が無い（シリアライザブルであるという保証が無い）。
selectのみのトランザクションであっても、必ずコミットして成功することを確認しなければならない。

（トランザクション分離レベルREAD_COMMITTED（既存のほとんどのRDBMSのデフォルト）では、あるテーブルから全件selectし（1回目の取得）、そのテーブルが別トランザクションによってinsertまたはdelete・コミットされた後に再び全件selectすると、更新後の（1回目より増減した）データが読まれる（2回目の取得）。すなわち、1回目と2回目で異なるデータがselectされる。SERIALIZABLEではこれは許されない（SERIALIZABLEでは、トランザクション内では常に同じデータが読まれなければならない）ので、こういう状態になるとシリアライゼーションエラーが発生する。逆に言えば、コミットが成功したなら、シリアライゼーションエラーが起きなかったということである）

Tsurugiでは、SQLを実行した時やトランザクションをコミットした時にそのトランザクションの内容が他のトランザクションと競合していないか確認しており、競合した場合はシリアライゼーションエラー（リトライ可能なアボート）が発生することがある。

シリアライゼーションエラー（リトライ可能なアボート）が発生した場合は、トランザクション内で行った処理を先頭から再実行（リトライ）すれば、トランザクションが成功することが期待できる。再実行の際はトランザクションオプションを変更してもよい。
（例えば、LTXで更新中のデータをOCCで読むとアボートするので、RTXで再実行するとLTX更新前のデータを読むのでアボートしない）

シリアライゼーションエラー（リトライ可能なアボート）発生時の再実行は、後述のトランザクションマネージャーで自動的に実施することが出来る。

#### コミットオプション

Tsurugiでは、トランザクションのコミット時にオプションを指定する。

DB内でどこまで処理したらcommitメソッドから制御が返るかを表す。

- DEFAULT
  - DB側の設定に従う。
- ACCEPTED
  - コミット操作が受け付けられた。
- AVAILABLE
  - コミットデータが他トランザクションから見えるようになった。
- STORED
  - コミットデータがローカルディスクに書かれた。
- PROPAGATED
  - コミットデータが適切な全てのノードに伝播された。

Iceaxeでは、コミットオプションはTgCommitType列挙型で表す。

```java
import com.tsurugidb.iceaxe.transaction.TgCommitType;

transaction.commit(TgCommitType.DEFAULT);
```

制限事項：コミットオプションは未実装。何かを指定する必要はあるが、効果は無い。

#### トランザクションの例

Iceaxeの基本的な使用方法としてはトランザクションマネージャーでトランザクションを管理することを想定しているが、コミットやロールバック・シリアライゼーションエラー発生時の制御等をユーザープログラマーが行いたい場合は、直接トランザクションを生成・使用することも出来る。

TsurugiTransactionインスタンスはTsurugiSessionのcreateTransactionメソッドで生成する。
トランザクションインスタンスを使い終わったら、クローズする必要がある。（明示的にクローズしない場合、セッションのクローズ時にクローズされる）

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

トランザクションマネージャーの内部では、上記のようなトランザクション生成・コミット・ロールバック・クローズ処理を行っている。

#### TsurugiTransactionの状態取得

TsurugiTransactionの状態を取得するメソッドのうち、主なもの。

| メソッド                                          | 取得内容                                                     | 備考                                                         |
| ------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| int getIceaxeTxId()                               | トランザクションに付けられた番号                             | トランザクションに関してIceaxe（JavaVM）内でユニークな番号。デバッガー等で識別するときに利用できる |
| TgTxOption getTransactionOption()                 | トランザクションオプション                                   |                                                              |
| TsurugiTransactionManager getTransactionManager() | トランザクションを生成したトランザクションマネージャー       | トランザクションマネージャー経由でない場合はnull             |
| int getIceaxeTmExecuteId()                        | トランザクションマネージャーのexecuteメソッドを実行する際に付けられた番号 | トランザクション実行に関してIceaxe（JavaVM）内でユニークな番号。シリアライゼーションエラーによって再実行しても変わらないので、一連のトランザクションを識別したいときに利用できる。トランザクションマネージャー経由でない場合は0 |
| int getAttempt()                                  | 試行番号（トランザクションマネージャーでの再実行回数）       | 初回は0で、シリアライゼーションエラーによって再実行する度に増えていく。トランザクションマネージャー経由でない場合は常に0 |
| String getTransactionId()                         | DB側が採番したトランザクションID                             | DBサーバー側のログでもこのトランザクションIDが出力されるので、紐付けるのに利用できる。トランザクションIDはtoStringメソッドが返す文字列内にもあるが、そちらは、DBから取得していない時点ではnull |
| boolean available()                               | トランザクションが有効かどうか（DBと通信可能かどうか）       | DBサーバー側でエラー（一意制約違反等）が発生した場合にトランザクションがinactiveになることがあるが、それはこのメソッドでは検知できない |
| boolean isCommitted()                             | トランザクションがコミットされたかどうか                     |                                                              |
| boolean isRollbacked()                            | トランザクションがロールバックされたかどうか                 |                                                              |



### トランザクションマネージャー

TsurugiTransactionManagerは、トランザクション内でシリアライゼーションエラー（リトライ可能なアボート）が発生したときに自動的に再実行を行う為のクラス。Iceaxe独自の機能。

TsurugiTransactionManagerのexecute系メソッド（トランザクションの実行）はスレッドセーフ。

#### トランザクションマネージャーの例

TsurugiTransactionManagerインスタンスはTsurugiSessionのcreateTransactionManagerメソッドで生成する。

トランザクションマネージャーは内部でトランザクションを生成するが、その際には、トランザクションマネージャーの生成に使われたセッションを使用する。

トランザクションマネージャーを生成する際か、トランザクションを実行する（executeメソッドを呼び出す）際のどちらかで、トランザクションマネージャーの設定（TgTmSetting）を渡す必要がある。

TgTmSettingでは、トランザクション生成に使用するトランザクションオプションやコミット時のコミットオプション等を設定できる。

ユーザープログラマーはTsurugiTransactionManagerのexecuteメソッドを呼び出してトランザクションを実行する。

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

TsurugiTransactionManagerのexecuteメソッドが呼ばれると、内部でトランザクションを生成する。
executeメソッドに渡す関数が、トランザクション1回分の処理となる。

- この関数では、トランザクションを受け取り、それを使ってSQLステートメントを実行する。
- この関数が正常に終了すると、トランザクションマネージャーはトランザクションをコミットする。
  - ただし、関数内でトランザクションに対して明示的にロールバックを実行した場合、トランザクションマネージャーはコミットを実行しない。
- この関数内で例外が発生すると、トランザクションマネージャーはトランザクションをロールバックする。
- シリアライゼーションエラー（リトライ可能なアボート）の例外が発生すると、トランザクションマネージャーは新しいトランザクションを生成して、この関数を再度実行する。そのため、この関数は冪等な処理でなければならない。

TsurugiTransactionManagerの例は、iceaxe-examplesのExample04TransactionManagerを参照。
トランザクションマネージャーでコミットオプションを指定しておく例は、iceaxe-examplesのExample91CommitTypeを参照。

#### トランザクションマネージャーでトランザクションオプションを指定する方法

トランザクションマネージャーを使用する場合は、初回や再実行時のトランザクション生成に使用するトランザクションオプションをTgTmTxOptionSupplierで定義し、TgTmSettingに入れておく。

どのようなエラーコードをシリアライゼーションエラーとするかの判定もTgTmTxOptionSupplierに設定できる。

再実行する際に、TgTmTxOptionSupplierで設定されることになる実行回数を超えた場合、トランザクションマネージャーはリトライ回数オーバーの例外（TsurugiTransactionRetryOverIOException）をスローする。

TgTmTxOptionSupplierを生成するメソッドは以下のようなものがある。

| メソッド                               | 説明                                                         | 実行回数                                   |
| -------------------------------------- | ------------------------------------------------------------ | ------------------------------------------ |
| of(txOption)                           | 初回のトランザクションオプションを指定する。                 | 1回（再実行はしない）                      |
| of(txOptions)                          | トランザクションオプションの一覧を指定する。n個目のトランザクションオプションがn回目の実行時に使われる。 | 指定されたトランザクションオプションの個数 |
| ofAlways(txOption)                     | 常に指定されたトランザクションオプションで実行される。       | Integer.MAX_VALUE回（実質、無限）          |
| ofAlways(txOption, attemtMaxCount)     | 常に指定されたトランザクションオプションで実行される。       | attemtMaxCount回                           |
| of(txOption1, size1, txOption2, size2) | txOption1をsize1回実行した後で、txOption2をsize2回実行する。 | size1＋size2 回                            |

TgTmSettingにも同様のメソッドがあり、こちらを使うとTgTmTxOptionSupplierの生成がソースコード上は省略できる。（TgTmSettingの内部でTgTmTxOptionSupplierを生成している）

```java
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
var tm = session.createTransactionManager(setting);
```

TgTmTxOptionSupplierおよびTgTmSettingの例は、iceaxe-examplesのExample04TmSettingを参照。



### DDLの実行方法

create tableやdrop table等のDDLを実行する際もトランザクションのAPIを使用する。
（DDLであっても、DB内部ではメタデータ登録の為にトランザクションIDが必要となるらしい）

しかし、DDLはトランザクションの管理外である。
例えば、create tableが成功した時点で（コミットしなくても）テーブルは作られ、ロールバックしてもテーブルは消えない。（ただし、現在未対応のcreate temporary tableでは、トランザクションが終了したらテーブルが消える予定）

制限事項：将来、DDLの実行もトランザクショナルになる予定。

制限事項：DDLを実行するトランザクションでは、DMLを実行してはならない。（現時点では、DB側の動作保証外）

制限事項：別トランザクションであっても、DDLとDMLを並行して実行してはならない。（現時点では、DB側の動作保証外。DBサーバーが無応答になったりクラッシュしたりすることがある（DML実行中のテーブルをdropした場合等））

#### DDLを実行する例

DDLはTsurugiTransactionのexecuteDdlメソッドで実行できる。

LTXでDDLを実行する場合、write preserve（更新対象テーブル）を指定する必要は無い。
制限事項：DDLを実行する旨のオプションを指定することになる予定

DDLの実行でも（システムテーブルへの書き込みで）シリアライゼーションエラーが発生する可能性があるので、リトライできるようにする必要がある。

```java
var setting = TgTmSetting.ofAlways(TgTxOption.ofLTX());
var tm = sesstion.createTransactionManager(setting);
tm.execute(transaction -> {
    transaction.executeDdl("create table TEST(FOO int primary key, BAR bigint, ZZZ varchar(10))");
});
```

ひとつしかDDLを実行しない場合は、トランザクションマネージャーのexecuteDdlメソッドを利用することも出来る。
通常はトランザクションマネージャーを生成する際にトランザクションオプションを指定するが、トランザクションマネージャーのexecuteDdlメソッドを使う場合は省略可能。（省略した場合はwrite preserve無しのLTXとなる）

```java
var tm = sesstion.createTransactionManager();
tm.executeDdl("create table TEST(FOO int primary key, BAR bigint, ZZZ varchar(10))");
```

DDLの例は、iceaxe-examplesのExample11Ddlを参照。

#### Iceaxeが対応しているデータ型

| create tableのデータ型 | Tsubakuroのデータ型（AtomType）     | Iceaxeのデータ型（TgDataType） | Javaのデータ型 |
| ------------------------ | ----------------------------------- | ------------------ | ------------------ |
|                   | BOOLEAN                    | BOOLEAN | boolean（Boolean） |
| INT                      | INT4                       | INT  | int（Integer）     |
| BIGINT                   | INT8                       | LONG   | long（Long）       |
| REAL                     | FLOAT4                     | FLOAT | float（Float）     |
| DOUBLE                   | FLOAT8                     | DOUBLE | double（Double）   |
| DECIMAL                  | DECIMAL                    | DECIMAL | BigDecimal         |
| CHAR・VARCHAR            | CHARACTER                  | STRING       | String             |
| BINARY・VARBINARY        | OCTET                      | BYTES        | byte[]             |
|                          | BIT                        | BITS      | boolean[]          |
| DATE                     | DATE                       | DATE      | LocalDate          |
| TIME                     | TIME_OF_DAY                | TIME      | LocalTime          |
| TIMESTAMP | TIME_POINT                 | DATE_TIME | LocalDateTime         |
| TIME WITH TIME ZONE | TIME_OF_DAY_WITH_TIME_ZONE | OFFSET_TIME | OffsetTime |
| TIMESTAMP WITH TIME ZONE | TIME_POINT_WITH_TIME_ZONE | OFFSET_DATE_TIME・ZONED_DATE_TIME | OffsetDateTime・ZonedDateTime |

制限事項：TIMESTAMP WITH TIME ZONEは、現時点ではタイムゾーンオフセットしか保持されない。（「Asia/Tokyo」のようなタイムゾーン情報は保持しない）

- IceaxeはZonedDateTimeに対応しているが、DB内ではタイムゾーンオフセットしか保持されないので、ZonedDateTimeで取得する場合はZoneIdを別途指定する必要がある。



### SQL（DML）の実行方法

SQL文は『SQLステートメント』（TsurugiSqlのサブクラス）で管理・実行する。

TsurugiSqlの具象クラスのインスタンスはTsurugiSessionから生成する。
使い終わったらクローズする必要がある。（明示的にクローズしない場合、セッションのクローズ時にクローズされる）

TsurugiSql系クラスは、SQL実行に関してスレッドセーフ。

SQLがクエリー（select文）か更新系SQL（insert・update・delete文）か、バインド変数が有るか無いかによって、具象クラスや生成する為のメソッドが異なる。

| SQLの内容                   | TsurugiSessionの生成メソッド                      | 返ってくる具象クラス        |
| --------------------------- | ------------------------------------------------- | --------------------------- |
| クエリー・バインド変数なし  | createQuery(sql, resultMapping)                   | TsurugiSqlQuery             |
| クエリー・バインド変数あり  | createQuery(sql, parameterMapping, resultMapping) | TsurugiSqlPreparedQuery     |
| 更新系SQL・バインド変数なし | createStatement(sql)                              | TsurugiSqlStatement         |
| 更新系SQL・バインド変数あり | createStatement(sql, parameterMapping)            | TsurugiSqlPreparedStatement |

- sql（必須）
  - SQL文の文字列
- parameterMapping（バインド変数を使う場合は必須）
  - バインド変数の変数名とデータ型の定義
- resultMapping
  - クエリー（select文）の実行結果のレコード（行）をユーザープログラマーが利用したいクラスに変換する為の定義
  - 省略時は、Iceaxeが用意しているTsurugiResultEntityに変換される。

実際には、バインド変数がある形式とは、TsubakuroのPreparedStatementを使う方式（事前にDB側にSQL文を登録しておく方式）の事であり、バインド変数が無い形式とは、毎回SQL文をDBに送信して実行する方式（すなわち実行する度にSQL文の解釈・コンパイルを行う）である。
バインド変数定義（parameterMapping）を空にすれば、バインド変数なしでもPreparedStatementを使うことが出来る。



#### SQLステートメントの実行方法

TsurugiTransactionのexecute系メソッドにSQLステートメント（TsurugiSqlインスタンス）を渡してSQLを実行する。
バインド変数がある場合は、その値をパラメーターとして渡す。

| 内容                          | SQL実行メソッド      | 備考                                                 |
| ----------------------------- | -------------------- | ---------------------------------------------------- |
| クエリー・全件取得            | executeAndGetList    | 全レコードのListを返す                               |
| クエリー・1件取得             | executeAndFindRecord | 1レコードをOptionalで返す                            |
| クエリー・1件ずつ処理         | executeAndForEach    | レコードを処理する関数を渡す                         |
| 更新系SQL                     | executeAndGetCount   | 制限事項：処理件数を返す想定だが、現在は常に-1を返す |
| クエリー・実行結果クラス取得  | executeQuery         | 実行結果クラス（TsurugiQueryResult）を返す           |
| 更新系SQL・実行結果クラス取得 | executeStatement     | 実行結果クラス（TsurugiStatementResult）を返す       |

SQLステートメントを実行する基礎的な方法は、実行結果クラスを取得する方法（executeQuery・executeStatementメソッド）である。
クエリーの場合は実行結果クラス（TsurugiQueryResult）からselect結果のレコードを取得する。
更新系SQLの場合は実行結果クラス（TsurugiStatementResult）を使って更新が成功したかどうかを確認する。（getUpdateCountメソッドを呼び出すと成功可否がチェックされる）
実行結果クラスは、使い終わったらクローズする必要がある。（明示的にクローズしない場合は、トランザクションのコミット前やロールバック前、あるいはクローズ時にクローズされる）

制限事項：クエリーに関して。全件読まずに途中でクローズしても、現在のTsubakuroの実装ではクローズ処理内で残りを全件読む。したがって、件数が多いselect文の場合、クローズ処理で時間がかかることがある。

SQLの実行完了を確認せずに次のSQLを実行すると、そのSQLはDB内部では前のSQLと並列に処理される可能性がある。SQLを順次実行したい場合は、必ずSQLの実行完了を確認してから次のSQLを実行開始する必要がある。
SQLの実行完了を確認する方法は以下の通り。

- クエリーの場合、全件読み終わること
  - すなわち、実行結果クラス（TsurugiQueryResult）のクローズが成功すること
- 更新系SQLの場合、更新の成功可否をチェックすること（更新が成功したなら、SQLの実行が完了している）
  - すなわち、実行結果クラス（TsurugiStatementResult）のgetUpdateCountメソッド呼び出しが正常終了すること、または実行結果クラスのクローズが成功すること

TsurugiTransactionのexecuteAnd系メソッドを使うと、更新の成功可否チェックや実行結果クラスのクローズはメソッド内部で処理される。

なお、トランザクションマネージャーにも同名のexecuteAnd系メソッドがあり、ひとつのトランザクションでひとつのSQLしか実行しない場合は、こちらを使うのも便利である。

SQLステートメントは、SQLステートメントが作られたセッションと同じセッションから作られたトランザクションに対してしか実行できない。（セッションがクローズされたらそのセッションで作られたSQLステートメントは使用不可になるので、別セッション（で作られたトランザクション）では使用できない）

クエリーの例はiceaxe-examplesのExample31Select, Example32Countを参照。
更新系SQLの例はiceaxe-examplesのExample21Insert, Example41Updateを参照。



#### バインド変数を使わない更新系SQLの例

```java
var sql = "update TEST set BAR = 1 where FOO = 123";
try (var ps = session.createStatement(sql)) {
    tm.execute(transaction -> {
        transaction.executeAndGetCount(ps);
    });
}
```

（バインド変数が無い場合、createStatementメソッドで返されるクラスはPreparedStatementではないが、慣例としてpsという名前の変数を使っている）

更新系SQLを実行するメソッドは、executeStatementとexecuteAndGetCountがある。
後者は処理件数を返すメソッドなので、処理件数が不要な場合は前者で実行したくなるかもしれないが、前者は実行結果クラスを返すメソッドであり、実行結果クラスは必ずクローズする必要がある。
そのため、基本的にはexecuteAndGetCountメソッドの使用を推奨する。



#### バインド変数の使用方法（クエリー・更新系SQL共通）

バインド変数は、SQL文の中に変数を埋め込んでおき、SQL実行時に外部から値を決定する仕組みである。

バインド変数の使い方は以下のようになる。

1. SQL文の中にコロンで始まる変数名（ `:変数名` という形式）を入れる。
   - 変数名に使える文字は、1文字目は英字またはアンダースコアで、2文字目以降は英数字またはアンダースコア。大文字小文字は区別される。
2. 変数名とそのデータ型をTgParameterMappingクラスで定義し、TsurugiSessionのcreateQueryやcreateStatementメソッドに渡してSQLステートメントを生成する。
3. 具体的な値（パラメーター）をTsurugiTransactionのexecute系メソッドに渡してSQLを実行する。

##### バインド変数の例（基本形）

バインド変数の変数名を、SQL文・TgParameterMapping・TgBindParametersでそれぞれ指定する方式。

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

TgBindVariablesは複数のバインド変数の定義を保持するクラス。
TgBindVariablesのadd系メソッドでバインド変数の変数名とデータ型を追加していき、TgParameterMappingに変換する。

TgBindParametersは、バインド変数に代入する値（パラメーター）を保持するクラス。
バインド変数に代入する値は、TgBindParametersのadd系メソッドで追加していく。
TgBindVariablesで指定したデータ型と同じデータ型のメソッドを使う必要がある。

##### バインド変数の例（TgBindVariableを使う方法）

TgBindVariableは、バインド変数1個の変数名とデータ型を保持するクラス。
TgBindVariableを使うと、バインド変数の変数名とデータ型の定義箇所を一か所だけにすることが出来る。

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

TgBindVariableの `ofデータ型` メソッドにバインド変数名を渡してインスタンスを生成する。

TgBindVariableのsqlNameメソッドは `:変数名` という文字列を返す。
なお、toStringメソッドは `:変数名/*データ型*/` というコメント付きの文字列を返すので、（コメントがネストしないなら）これをSQL文に使うことも出来る。

バインド変数に代入する値は、TgBindVariableのbindメソッドで指定する。
（bindメソッドの引数は、定義時に決められたデータ型しか受け付けないので、他の型で指定してしまうようなミスを起こしにくい）

##### バインド変数の例（変数が1つだけの場合）

バインド変数を1つだけしか使用しない場合は、TgParameterMappingに変数名とデータ型を指定することで、パラメーターに値を直接指定することが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var sql = "update TEST set BAR = 1 where FOO = :foo";
var parameterMapping = TgParameterMapping.of("foo", int.class);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        int parameter = 123;
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

##### バインド変数の例（Entityを変換する方法）

ユーザープログラマーが用意するEntityクラス（getterメソッドを持つクラス）に対し、各getterメソッドで取得した値をバインド変数に割り当てることが出来る。

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

TgParameterMappingのofメソッドに、パラメーターとして使いたいクラスを指定する。その後、add系メソッドでバインド変数名とgettterメソッドを追加していく。

実行時のパラメーターには、TgParameterMappingのofメソッドで指定したクラスのオブジェクトが直接指定できる。

###### バインド変数の例（TgBindVariableを使ってEntityを変換する方法）

TgBindVariableを使ってEntityクラスの変換定義を記述することが出来る。

```java
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var foo = TgBindVariable.ofInt("foo");
var bar = TgBindVariable.ofLong("bar");
var zzz = TgBindVariable.ofString("zzz");

var sql = "insert into TEST (FOO, BAR, ZZZ)"
    + " values(" + TgBindVariables.of(foo, bar, zzz).getSqlNames() + ")";
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

TgParameterMappingのaddメソッドを使い、第1引数にTgBindVariableを渡す。

TgBindVariablesのgetSqlNamesメソッドは、`:変数名`形式のバインド変数名をカンマ区切りで結合した文字列を返す。

###### バインド変数の例（TgBindVariablesと変換関数を用意してEntityを変換する方法）

EntityクラスからTgBindParametersに変換する関数をTgParameterMappingに登録しておく方式。

```java
import java.util.function.Function;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;

var variables = TgBindVariables.of().addInt("foo").addLong("bar").addString("zzz");
var sql = "insert into TEST (FOO, BAR, ZZZ) values(" + variables.getSqlNames() + ")";
Function<TestEntity, TgBindParameters> parameterConverter = entity -> TgBindParameters.of().add("foo", entity.getFoo()).add("bar", entity.getBar()).add("zzz", entity.getZzz());
var parameterMapping = TgParameterMapping.of(variables, parameterConverter);
try (var ps = session.createStatement(sql, parameterMapping)) {
    tm.execute(transaction -> {
        var parameter = new TestEntity(123, 456L, "abc");
        transaction.executeAndGetCount(ps, parameter);
    });
}
```

変換関数の他に、バインド変数の定義（TgBindVariables）は必要。



#### クエリーの使用方法（バインド変数の有無に関わらず共通）

クエリー（select文）を実行すると、DBから（複数の）レコードが返ってくる。
返ってきたレコードは、ユーザプログラマーが用意するEntityクラス（setterメソッドを持つクラス）に変換される。この変換の定義をTgResultMappingクラスで行う。

ユーザープログラマーがEntityクラスを用意する代わりにTsurugiResultEntityクラスを使うことも出来る。
TsurugiSessionのcreateQueryメソッドで引数のresultMappingを省略すると、TsurugiResultEntityに変換されるようになる。

##### 結果取得の例（TsurugiResultEntityを使用する方法・カラム名指定）

Iceaxeが用意しているTsurugiResultEntityを使う例。

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

TsurugiResultEntityのgetterメソッドにカラム名を指定して値を取得する。

TsurugiResultEntityのgetterメソッドには以下のような種類がある。

| メソッド                        | 説明                                                         |
| ------------------------------- | ------------------------------------------------------------ |
| getデータ型(name)               | 値を取得する。値がnullの場合はNullPointerExceptionが発生する。 |
| getデータ型(name, defaultValue) | 値を取得する。値がnullの場合はdefaultValueが返る。           |
| getデータ型orNull(name)         | 値を取得する。値がnullの場合はnullが返る。                   |
| findデータ型(name)              | 値をOptionalで取得する。                                     |

##### 結果取得の例（TsurugiResultEntityを使用する方法・カラムの並び順依存）

TsurugiResultEntityからカラムの位置（index）を指定して値を取得する例。

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

TsurugiResultEntityのgetNameメソッドで、指定された位置のカラム名を取得できる。
select文で明示的にカラム名が指定されていない場合は、Iceaxeが適当な名前を生成する。

なお、TsurugiResultEntityのgetNameListメソッドでカラム名一覧を取得できる。

##### 結果変換の例（Entityに変換する方法・カラム名指定）

ユーザプログラマーが用意するEntityクラス（setterメソッドを持つクラス）に変換する例。

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

TgResultMappingのofメソッドにEntityのコンストラクターを渡し、add系メソッドでカラム名とsetterメソッドを追加していく。

##### 結果変換の例（Entityに変換する方法・カラムの並び順依存）

TgResultMappingのadd系メソッドでカラム名を省略すると、カラムの並び順依存になる。

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

##### 結果変換の例（1カラムだけ取得する場合）

1カラムだけしか取得しない場合は、TgResultMappingにカラムのデータ型を指定することで、その値を直接取得することが出来る。

```java
import java.util.Optional;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;

var sql = "select count(*) from TEST";
var resultMapping = TgResultMapping.of(int.class);
try (var ps = session.createQuery(sql, resultMapping)) {
    tm.execute(transaction -> {
        Optional<Integer> countOpt = transaction.executeAndFindRecord(ps);
        int count = countOpt.get();
    });
}
```

（この例ではselect結果は常に1件なので、executeAndFindRecordメソッドを使い、返ってきたOptionalがemptyかどうかを確認せずに値を取り出している）

##### 結果変換の例（TsurugiResultRecordから変換する方法）

TsurugiResultRecordクラスは、Iceaxeが1レコード分のデータを処理するクラス。（使用上の注意点については後述）

このTsurugiResultRecordからEntityへ変換する関数を用意する例。

```java
import java.io.IOException;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

class TestEntity {
    public static TestEntity of(TsurugiResultRecord record) throws IOException, TsurugiTransactionException {
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

TgResultMappingのofメソッドに、TsurugiResultRecordからEntityへ変換する関数を渡す。



#### TsurugiQueryResult・TsurugiResultRecordの注意点

TsurugiQueryResultは、クエリー（TsurugiSqlQuery・TsurugiSqlPreparedQuery）を実行したときに作られる、select結果を受け取るクラスである。
TsurugiResultRecordはTsurugiQueryResult内部で使用される、1レコード分のデータを処理するクラスである。
（TsurugiQueryResultとTsurugiResultRecordは連動してTsubakuroのResultSetクラスの処理を行う）

Iceaxeの大多数のクラスと異なり、TsurugiQueryResultとTsurugiResultRecordは**スレッドセーフではない**。

TsurugiQueryResultはselect結果の複数レコードを先頭から順番に処理していく。各レコードは一度しか読むことが出来ない。

TsurugiResultRecordは1レコード分のデータを処理するが、インスタンスはTsurugiQueryResultの中でひとつしか生成されず、複数レコード間で同じインスタンスが使い回される。また、TsurugiQueryResultをクローズすると、TsurugiResultRecordも使用できなくなる。
したがって、ユーザープログラマーが各レコードの値を保存する目的でTsurugiResultRecordインスタンスを保持する（Listにaddしていく）ような使い方は出来ない。また、トランザクションの外にTsurugiResultRecordを出すことも出来ない（トランザクションが終わるということはトランザクションがクローズされているということであり、トランザクションのクローズ時にTsurugiQueryResultもクローズされる為）。
そのため、TsurugiQueryResultからレコードを返す際は、TsurugiResultRecordを必ずEntityクラス（1レコード分の値を保持するクラス）に変換している。（レコード毎にEntityインスタンスを生成する）
（TsurugiResultRecordをトランザクションの外に出さないのであれば、必ずしもEntityクラスに変換しなくてもよい。変換しない分、処理速度は向上する）

TgResultMappingを使うと、TsurugiResultRecordの処理が隠蔽されるので、上記の注意点をユーザープログラマーが気にする必要は無い。（TsurugiResultRecordから変換する関数を書く場合を除く）
TsurugiTransactionのexecuteAnd系メソッドを使うと、TsurugiQueryResultの処理が隠蔽されるので、上記の注意点をユーザープログラマーが気にする必要は無い。

##### TsurugiResultRecordの使用方法

TsurugiResultRecordから値を取得するメソッド群は、3種類に分類される。ある群のメソッドを使用したら、他の群のメソッドは基本的に使用不可。

###### 現在カラム系

メソッド名に `CurrentColumn` が入っているメソッド群。

select文の複数カラムに対し、moveCurrentColumnNextメソッドによって現在カラムを移動しながら順番に値を取得していく。各カラムの値は一度しか取得できない。

select文のカラム数やデータ型を気にせず汎用的に取得する方法である。

```java
while (record.moveCurrentColumnNext()) { // カラムが存在している間ループ
    String name = record.getCurrentColumnName(); // 現在位置のカラム名取得
    Object value = record.fetchCurrentColumnValue(); // 現在位置の値取得
}
```

###### カラム名指定系

メソッド名が `getデータ型` あるいは `findデータ型` で始まっているメソッド群。

select文のカラム名を指定して値を取得する。

```java
entity.setFoo(record.getIntOrNull("FOO"));
entity.setBar(record.getLongOrNull("BAR"));
entity.setZzz(record.getStringOrNull("ZZZ"));
```

（このメソッドを使用した場合、内部では一旦全カラムを読み込んで、カラム名をキーとするMapを作っている。（他のメソッド群ではこういったMapは作っていない）（したがって、他のメソッド群と共存できない））

###### next系

メソッド名が `nextデータ型` で始まっているメソッド群。

select文の複数カラムに対し、順番に値を取得しながら現在カラムを移動していく。各カラムの値は一度しか取得できない。

next系メソッドを呼んだ直後にgetCurrentColumnName（現在位置のカラム名取得）, getCurrentColumnType（現在位置のデータ型取得）メソッドは使用可能。

カラム名を使用せず、select文のカラムの並び順とカラム数に依存した取得方法である。

```java
entity.setFoo(record.nextIntOrNull());
entity.setBar(record.nextLongOrNull());
entity.setZzz(record.nextStringOrNull());
```



## その他の機能

### テーブルメタデータの取得

テーブルメタデータ（テーブルの定義情報）を取得することが出来る。

```java
import java.util.Optional;
import com.tsurugidb.iceaxe.metadata.TgTableMetadata;

Optional<TgTableMetadata> metadata = session.findTableMetadata("TEST");
```

TsurugiSessionのfindTableMetadataメソッドにテーブル名を指定する。
返ってくるのはOptionalであり、テーブルが存在しない場合はemptyになる。すなわち、テーブルの存在確認に使える。

制限事項：TgTableMetadataにはデータベース名やスキーマ名を取得するメソッドがあるが、現在は未実装。

制限事項：TgTableMetadataからカラム一覧を取得することが出来るが、現時点では情報量は少ない（データ型の詳細やキー情報・インデックス情報等は取得できない）。



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

実行計画の取得にトランザクションは無関係であり、SQLステートメント（TsurugiSqlの具象クラス）のexplainメソッドでSQLのメタデータを取得する。
バインド変数がある場合は、explainメソッドの引数にSQLのパラメーター（具体的な値）を渡す。

実行計画を取得する例は、iceaxe-examplesのExample61Explainを参照。

制限事項：バインド変数が無いSQL（TsurugiSqlQueryとTsurugiSqlStatement）では、実行計画取得は未実装（UnsupportedOperationExceptionが発生する）



### タイムアウト時間の設定

DBと通信する箇所では、タイムアウト時間を指定できる。

※実際的には、デフォルトのタイムアウト時間と、コミットのタイムアウト時間くらいしか指定することは無いと思われる。（コミットは、トランザクションの大きさによっては、かなりの時間がかかることがある）

タイムアウト時間を指定する例はiceaxe-examplesのExample92Timeoutを参照。

#### セッションオプションによるタイムアウト指定

タイムアウト時間は、セッション生成時に指定するセッションオプションで指定できる。

どの箇所のタイムアウト時間かは、TgTimeoutKey列挙型で指定する。
TgTimeoutKey.DEFAULTで、全ての箇所で共通のデフォルトのタイムアウト時間を指定できる。デフォルトのタイムアウト時間を指定しなかった場合は、Long.MAX_VALUE[ナノ秒]になる（実質、無限待ち）。

##### セッションオプションでタイムアウトを指定する例

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

DBと通信を行うクラスでは、タイムアウト時間を指定するメソッドがある。

##### TsurugiTransactionでタイムアウトを指定する例

```java
import java.util.concurrent.TimeUnit;

tm.execute(transaction -> {
    transaction.setCommitTimeout(1, TimeUnit.HOURS);
    ～
});
```

#### トランザクションマネージャーによるタイムアウト指定

トランザクションマネージャーで生成するトランザクションに対しては、トランザクションマネージャーの設定でタイムアウト時間を指定することが出来る。

##### トランザクションマネージャーでタイムアウトを指定する例

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

select結果を保持したりバインド変数に変換したりする為にユーザープログラマーが用意するEntityクラスにおいて、テーブルではvarcharやint等で扱っているデータを、Entityでは列挙型で保持する場合の変換方法について。

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

#### TgParameterMapping・TgResultMappingに変換関数を渡す方法

TgParameterMappingやTgResultMappingクラスのadd系メソッドにおいて、Entityのgetter/setterメソッドの他に、値を変換する関数を渡すことが出来る。

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

add系メソッドは、DBで保存するデータ型のメソッドを使用する。

TgParameterMappingでは、第3引数にEntityのデータ型（列挙型）からテーブルのデータ型へ変換する関数を渡す。
TgResultMappingでは、第3引数にテーブルのデータ型からEntityのデータ型（列挙型）へ変換する関数を渡す。

#### TgBindVariableを拡張する方法

バインド変数に関しては、TgBindVariableを継承して独自のクラスを作り、それを使ってEntityから変換することが出来る。

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

TgParameterMappingのaddメソッドにTgBindVariableを渡す方法を用いる。



### トランザクション再実行判定処理のカスタマイズ

TgTmTxOptionSupplier（トランザクションマネージャーでトランザクションを生成する際のトランザクションオプションを提供するクラス）は、これを継承して独自のクラスを作ることが出来る。

```java
class MyTxOptionSupplier extends TgTmTxOptionSupplier {

    @Override
    protected TgTmTxOption computeFirstTmOption() {
        return TgTmTxOption.execute(TgTxOption.ofOCC());
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(int attempt, TsurugiTransactionException e) {
        return TgTmTxOption.execute(TgTxOption.ofLTX("table1", "table2"));
    }

    @Override
    protected boolean isRetryable(TsurugiTransaction transaction, TsurugiTransactionException e) {
        return super.isRetryable(transaction, e);
    }
}

var setting = TgTmSetting.of(new MyTxOptionSupplier());
```

isRetryableメソッドで、再実行可能かどうか（発生した例外がシリアライゼーションエラー（リトライ可能なアボート）かどうか）を判定する。

isRetryableメソッドのデフォルトでは、TgTmTxOptionSupplierのsetRetryPredicateメソッドでセットされた判定関数が呼ばれる。
判定関数を明示的にセットしていない場合は、TsurugiDefaultRetryPredicateクラスのシングルトンインスタンスが使われる。このシングルトンインスタンスは変更可能なので、Iceaxe全体で再実行判定を変更したい場合は、ここを差し替えればよい。

computeFirstTmOptionメソッドで初回実行用のトランザクションオプション（TgTmTxOption）を返し、computeRetryTmOptionメソッドで再実行用のトランザクションオプション（TgTmTxOption）を返す。
computeRetryTmOptionメソッドは、isRetryableメソッドによって再実行可能と判定されたときのみ呼ばれる。

TgTmTxOptionはTgTmTxOptionSupplierからトランザクションオプションを返す為に使用するクラスで、以下のような生成メソッドを持つ。

| 生成メソッド      | 説明                                                         |
| ----------------- | ------------------------------------------------------------ |
| execute(txOption) | トランザクションが実行可能な場合に、使用するトランザクションオプションを保持する。 |
| retryOver()       | 再実行回数を超えた（リトライ回数オーバーになった）ことを表す。 |
| notRetryable()    | トランザクションが再実行できない（シリアライゼーションエラーでない例外が発生した）ことを表す。 |



### イベントリスナー

Iceaxeで処理を行うクラス（TsurugiConnectorやTsurugiSession・TsurugiTransaction・TsurugiTransactionManager、TsurugiSqlのサブクラス等）では、処理を行った際に呼ばれるイベントリスナーを登録することが出来る。
イベントリスナーは、基本的に、対象クラス名の末尾に `EventListener` を付加した名前のインターフェースである。（Javaが標準で提供している関数型インターフェースを使っている箇所もある）

処理件数をカウントしたり、ログを出力したり、インスタンスが生成された時にそれに対して初期設定を追加したりするのに利用できる。

イベントリスナーは、対象クラスのaddEventListenerメソッドで登録する。

```java
import com.tsurugidb.iceaxe.transaction.manager.event.TsurugiTmEventListener;

// トランザクションマネージャーでリトライされたときにログを出力する例
var tm = session.createTransactionManager(setting);
tm.addEventListener(new TsurugiTmEventListener() {
    @Override
    public void transactionRetry(TsurugiTransaction transaction, Exception cause, TgTxOption nextTxOption) {
        LOG.info("transaction retry: {}", cause.getMessage());
    }
});
```

イベントリスナーを使って実装されている例として、以下のようなクラスがある。

| 内容                                                         | クラス名                            | イベントリスナー            |
| ------------------------------------------------------------ | ----------------------------------- | --------------------------- |
| トランザクションマネージャーの各処理の実行件数をカウントする | TgTmSimpleCounter・TgTmLabelCounter | TsurugiTmEventListener      |
| トランザクションに関するログを出力する                       | TsurugiSessionTxFileLogger          | TsurugiSessionEventListener |



### IceaxeConvertUtil

TgParameterMappingやTgResultMapping（TsurugiResultRecordやTsurugiResultEntity）において、ユーザープログラマーが使用するデータ型とTsubakuroのデータ型との変換を行う際に使われるのがIceaxeConvertUtilクラスである。

独自の変換を行いたい場合は、IceaxeConvertUtilを継承したクラスを作り、TgParameterMappingやTgResultMappingに登録することが出来る。

```java
import com.tsurugidb.iceaxe.util.IceaxeConvertUtil;

static final IceaxeConvertUtil MY_CONVERT_UTIL = new IceaxeConvertUtil() {

    @Override
    protected String convertString(Object obj) {
        if (obj instanceof Double) {
            return String.format("%.2f", obj);
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

IceaxeConvertUtilの例は、iceaxe-examplesのExample94TypeConvertを参照。



## 制限事項

2023-03-31時点でのIceaxeの制限事項。

- TsurugiTransactionのexecuteAndGetCountメソッドの戻り値は、更新系SQLで処理された件数を返す想定だが、現時点ではDB側にその実装が無いため、常に-1を返す。
  - executeAndGetCountメソッド内で、更新系SQLの処理が完了したかどうかの確認は行われる。

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
- TsurugiTransactionのexecuteDdl()・バインド変数なしのexecuteQuery(), executeStatement()を実行して、DB側で（文法エラー等の）エラーが発生した際に、エラーメッセージに詳細情報が含まれない。
  - 「error in db_->create_executable()」というエラーメッセージが返る。（PreparedStatement（バインド変数あり）の場合は詳細なエラーメッセージが返る）
- バインド変数が無いSQL（TsurugiSqlQueryとTsurugiSqlStatement）では、実行計画の取得処理（explainメソッド）は未実装。
  - 使用すると、TsubakuroでUnsupportedOperationExceptionが発生する。
- コミット処理中にタイムアウト等の例外が発生してロールバックを呼び出しても、ロールバックされない。
  - Tsubakuroでは、コミットを開始した後にロールバックを呼んでも、何もせずに正常終了を返す。
- クライアント（Tsubakuroを使用するアプリケーション）を強制終了すると、DB側はクリーンアップされないので、トランザクション等が残り続ける。
