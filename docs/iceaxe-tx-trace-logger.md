# Iceaxeトランザクショントレースログ出力方法

この文書では、Iceaxeで出力できるトランザクションのトレースログについて説明する。



## 概要

IceaxeのTsurugiSessionTxFileLoggerクラスで、以下のようなトランザクションの実行状況をログファイルに出力できる。

- トランザクションの各処理の実行時刻
  - トランザクションの開始・コミット・ロールバック・クローズ
  - SQLの実行開始・終了
- SQLの実行計画

TsurugiSessionインスタンス生成時にTsurugiSessionTxFileLoggerを登録することで、そのセッションに対してこの機能が使用できる。
システムプロパティーの設定により、セッション生成時に自動的にTsurugiSessionTxFileLoggerを登録することが出来る。

ひとつのトランザクションに対してひとつのトレースログファイルが出力される。
実行計画のJSONをファイルに出力する場合は、SQLを実行する度にひとつのJSONファイルが出力される。

この機能を有効にすると、（実行計画を出力しない場合で）実行速度が3割程度低下する。
実行計画を出力すると実行速度が2～3倍遅くなる。



## 使用方法

JavaVMのシステムプロパティー（javaコマンドの`-D`）でTsurugiSessionTxFileLoggerの設定を行うことにより、全てのセッションでトランザクションのトレースログが出力されるようになる。

- `iceaxe.tx.log.dir`
  - トレースログの出力先ディレクトリーを指定する。
    - Iceaxeを使用する他のアプリケーションを同時に実行する場合は、異なるディレクトリーを指定する必要がある。（トレースログのファイル名が重複する可能性がある為）
  - これを設定していない場合は（自動的には）トレースログは出力されない。
- `iceaxe.tx.log.sub_dir`
  - ログ出力先の下に作成するサブディレクトリーの形式を指定する。
    - `NOTHING`
      - サブディレクトリー無し。
    - `TM`
      - トランザクションマネージャーのexecuteメソッド呼び出し毎にサブディレクトリーを作成する。
        - トランザクションマネージャーを介さずに作られたトランザクションの場合、全て「tm0」というサブディレクトリーになる。
      - トランザクションマネージャーによってトランザクションがリトライされた場合に、リトライされたトランザクションのログファイルが同じサブディレクトリーに入ることになる。
    - `TX` （デフォルト）
      - トランザクション毎にサブディレクトリーを作成する。
    - `TM_TX`
      - トランザクションマネージャーのexecuteメソッド呼び出し毎にサブディレクトリーを作成し、その下にトランザクション毎のサブディレクトリーを作成する。
- `iceaxe.tx.log.write_sql_file`
  - TsurugiSqlを作成する毎にSQL文をファイルに出力するかどうか。
    - `true`
      - 出力する。
      - 出力する場合、`出力先ディレクトリー/sql_statement/ss-N.sql`というファイルが出力される。
    - `false` （デフォルト）
      - 出力しない。
- `iceaxe.tx.log.header_format`
  - ログの各行の先頭に出力する時刻のフォーマット。
    - java.time.format.DateTimeFormatter#ofPattern()に渡す文字列。
  - デフォルトは`HH:mm:ss.SSS`
- `iceaxe.tx.log.sql_max_length`
  - SQLを実行する際にログ出力するSQL文の最大文字数。
  - 負の数（デフォルト）
    - SQL文を全て出力する。
  - `0`
    - SQL文を出力しない。
  - 正の数
    - その文字数まで出力する。
- `iceaxe.tx.log.arg_max_length`
  - SQLを実行する際にログ出力する引数（バインド変数）の最大文字数。
  - 負の数（デフォルト）
    - 引数を全て出力する。
  - `0`
    - 引数を出力しない。
  - 正の数
    - その文字数まで出力する。
- `iceaxe.tx.log.explain`
  - 実行計画の出力有無を指定する。
    - 実行計画を出力する場合、SQLを実行する度に出力する。
  - 以下の値の論理和で出力先を指定する。`0`だと出力しない。デフォルトは `2`。
    - `1`
      - ログファイル内にPlanGraphを出力する。
    - `2`
      - 実行計画のJSONファイルを出力する。
      - このJSONファイルを人間が見て分かりやすい形式に変換する方法は、後述の『実行計画のJSONファイルの変換』を参照。
- `iceaxe.tx.log.record`
  - select文で読み込んだレコードの内容をログファイルに出力するかどうか。
    - `true`
      - 出力する。
    - `false` （デフォルト）
      - 出力しない。
- `iceaxe.tx.log.read_progress`
  - select文の読み込みの進捗状況を出力する件数。この件数毎にログを出力する。
  - デフォルトは`0`（出力しない）
- `iceaxe.tx.log.auto_flush`
  - ログファイルの自動フラッシュ（1行出力する度にフラッシュする）を行うかどうか。
    - `true`
      - 自動フラッシュを行う。
    - `false` （デフォルト）
      - 自動フラッシュを行わない。

### 例

```bash
java -Diceaxe.tx.log.dir=/tmp/iceaxe-tx-log -Diceaxe.tx.log.explain=1 ～
```

> **Note**
>
> これらのシステムプロパティーの処理はTsurugiSessionTxFileLogConfigで行われ、TsurugiConnectorのcreateSessionメソッドで設定される。



## トレースログを出力するタイミング

トレースログを出力する契機（イベントが発生するクラス）は以下の通り。

### トランザクション

トランザクション（TsurugiTransaction）からは以下のタイミングでログを出力する。

- トランザクションの開始時
  - トランザクション開始時にトレースログファイルがオープンされる。（トランザクション毎に1ファイル）
  - 厳密には、TsurugiTransactionクラスではなく、TsurugiSession#createTransaction()。
- トランザクションIDの取得時
  - トランザクションIDはDB側から渡されるので、トランザクション生成および通信が成功しないと取得できない（すなわち、トランザクション開始直後はトランザクションIDは不明）
  - 「トランザクションIDの取得時」とは、厳密には、初回のSQL実行やコミット・ロールバックを行う直前である。したがって、それらを行わずにトランザクションをクローズした場合は、トランザクションIDは取得されない。
- SQLの実行開始・終了時
  - TsurugiTransaction#executeAndGetList()やexecuteAndGetCount()等の開始・終了時
- コミットの開始前・終了後
- ロールバックの開始前・終了後
- トランザクションのクローズ時
  - トランザクションがクローズされるとトレースログファイルもクローズするので、それ以降にさらにクローズが呼ばれた場合、ログファイルには出力されない。

### SQL（TsurugiSql）

TsurugiSql（TsurugiSqlPreparedQuery・TsurugiSqlPreparedStatement等）からは以下のタイミングでログを出力する。

- SQLの実行開始時
  - TsurugiSqlPreparedQuery#execute()やTsurugiSqlPreparedStatement#execute()等の開始時
  - 実行計画を出力する場合は、SQLの実行開始時に出力する（SQLのパラメーターによって実行計画が変わる可能性があるので、実行時に毎回出力する）

SQLの実行が開始されると『SQL実行結果』が返されるので、TsurugiSqlには「終了時のログ出力」は無い。

### SQL実行結果

SQL実行結果（TsurugiQueryResult・TsurugiStatementResult）からは以下のタイミングでログを出力する。

- TsurugiQueryResultからの読み込み時
- TsurugiQueryResultからの読み込みエラー発生時
- SQL実行の終了時
  - TsurugiQueryResultの場合、それ以上読み込むレコードが無くなった時や、読むのを途中で止めた時
  - TsurugiStatementResultの場合、結果チェックを実施した時
- SQL実行結果のクローズ時
  - クローズ時と終了時が分かれている理由は、Tsubakuroのクローズ処理で時間がかかる可能性がある為。
  - SQL実行結果がクローズされるとトレースログ出力用のオブジェクトを削除するので、それ以降にさらにクローズが呼ばれた場合、ログファイルには出力されない。

### トランザクションマネージャー

トランザクションマネージャー（TsurugiTransactionManager）からは以下のタイミングでログを出力する。
（アプリケーションがトランザクションマネージャーを使っている場合のみ）

- executeメソッドで実行した処理（アプリケーションの処理）で例外が発生した時
- トランザクションのリトライが発生した時

トランザクションマネージャーでトランザクションの生成やコミット・ロールバック・クローズを行うが、トランザクションマネージャーからはそれらのログを出力しない。

> **Note**
>
> トランザクションの生成やコミット・ロールバックの実行等は、「トランザクションのイベント」としてログが出力されるので、トランザクションマネージャーから出力する必要は無い。
>
> また、トランザクションはトランザクションマネージャーを使わなくても生成できるので、トランザクションマネージャーが無くても問題ないようにしている。

ただし、シリアライゼイションエラー（リトライ可能なアボート）によってリトライが発生した場合はログを出力する。
（リトライするかどうかの判定を行っているのはトランザクションマネージャーだから）

また、「executeメソッドで実行した処理（Iceaxeを利用しているアプリケーションの処理）」の中で例外が発生した場合もログを出力する。

> **Note**
>
> トランザクションのイベントでは、SQL実行で発生した例外しか捕捉できない。（SQLを実行したアプリケーション側の例外は、トランザクションでは捕捉できない）
>
> しかしアプリケーション側で例外が発生してトランザクションがロールバックされる場合は、例外が発生したことがログに出力されないと、なぜロールバックされたのかが分からない。
>
> （無論、アプリケーションがトランザクションマネージャーを使っていない場合は、例外のログは出せない）



## トレースログの例

### トレースログのファイル名の例

```
tx20230225_085504_793636.3.main.log
```

- トレースログのファイル名は「tx」で始まる。
- 「20230225_085504_793636」はトランザクションの開始日時。
- 次の「3」は、Iceaxe内で採番されたトランザクション番号（iceaxeTxId）
  - DBが採番するトランザクションIDとは異なる
- 「main」はスレッド名

ファイル名にiceaxeTxIdが入っているので、他のトランザクションと重複することは無い。
（ただし、Iceaxeを使用する他のアプリケーション（別プロセス）が同じ場所に出力すると重複する可能性はある）
日時が先頭に入っているのは、大抵のファイルシステムではその順で表示される為。

トランザクション毎にサブディレクトリーを作成する設定になっている場合、そのサブディレクトリー名はファイル名と同じ。

### トレースログの内容の例

#### トランザクションの開始

```
08:55:04.795 [TX-3] transaction start 2023-02-25T08:55:04.793636900+09:00[GMT+09:00] main
TsurugiTransaction(OCC{}, iceaxeTxId=3, iceaxeTmExecuteId=3, attempt=0, transactionId=null)
```

トレースログファイルの先頭に、トランザクションの開始メッセージが出力される。

`[TX-N]`のNは、iceaxeTxId。（下記参照）
「transaction start」の後ろに開始日時とスレッド名が出力される。

その直後の行はトランザクションの内容（TsurugiTransaction#toString()）。

- トランザクションオプション
- iceaxeTxId
  - トランザクションを生成する毎にIceaxeで採番されるトランザクション番号（JavaVM内でトランザクションとして一意となる連番）
    - DBが採番するトランザクションIDとは異なる
- iceaxeTmExecuteId
  - トランザクションマネージャーのexecuteメソッド呼び出し毎にIceaxeで採番されるID（JavaVM内でexecute呼び出しとして一意となる連番）
    - トランザクションマネージャー経由でない場合は0
- attempt
  - トランザクションの試行回数。シリアライゼイションエラー（リトライ可能なアボート）が発生してリトライする度に増えていく。初回は0
    - トランザクションマネージャー経由でない場合は0
- transactionId
  - DBが採番したトランザクションID。トランザクション開始時点では常にnull
    - Iceaxeがトランザクションを開始した時点ではまだ値が採番されていない（DBとの通信を行っていない）為

#### SQL実行開始

```
08:55:04.798 [TX-3][iceaxeTxExecuteId=3] executeAndGetCount(sql) start
08:55:04.798 [sql-3][ss-3] sql start. sql=insert into test(foo, bar, zzz)values(:foo, :bar, :zzz)
08:55:04.806 [sql-3][ss-3] args=TestEntity{foo=1, bar=1, zzz=1}
08:55:05.269 [sql-3][ss-3] PlanGraph [nodes=[{kind=write, title=write, attributes={write-kind=insert, table=test}}, {kind=values, title=values, attributes={}}]]
```

TsurugiTransactionのexecuteXxxメソッド（上記の例ではexecuteAndGetCount）を開始したログが出力される。
iceaxeTxExecuteIdは、executeXxxメソッド呼び出し毎にIceaxeで採番されるID。（JavaVM内でexecuteXxx呼び出しとして一意となる連番）

「sql start」は、TsurugiSqlでSQLの実行を開始したことを表す。

`[sql-N]`のNは、iceaxeSqlExecuteId。（SQL実行毎にIceaxeで採番されるID。JavaVM内でSQL実行として一意となる連番）
（常にシングルスレッドで実行されればiceaxeSqlExecuteIdとiceaxeTxExecuteIdは同じ値になるが、マルチスレッドならすぐずれる）

`[ss-N]`のNは、iceaxeSqlId。（TsurugiSql作成毎にIceaxeで採番されるID。JavaVM内でTsurugiSqlとして一意となる連番）
TsurugiSqlをファイル出力する設定になっている場合、`出力先ディレクトリー/sql_statement`の下に「ss-N.sql」というファイル名で出力される。

startの行に、実行するSQL文が出力される。
SQL文が指定された最大文字数より大きい場合、それ以降はカットされる。
最大文字数が0だった場合、SQL文は出力されない。
最大文字数が負の数だった場合、SQL文は全て出力される。

SQL文の次の行に引数（バインド変数の内容）が出力される。
アプリケーションから渡された引数オブジェクトのtoString()が出力されるので、toStringメソッドが実装されている必要がある。
引数を文字列化したものが指定された最大文字数より大きい場合、それ以降はカットされる。
最大文字数が0だった場合、引数は出力されない。
最大文字数が負の数だった場合、引数は全て出力される。

実行計画をログファイルに出力する設定になっている場合は、実行計画（PlanGraph）も出力される。
実行計画をJSONファイルに出力する設定の場合、ファイル名は「sql-N.explain.json」となる。
なお、実行計画取得・出力中にエラーが発生した場合は、その旨をログに出力し、処理を継続する。（実行計画の処理に失敗しても、全体の処理を停止させない）

#### トランザクションID

```
08:55:05.272 [TX-3] lowTransaction get start
08:55:05.273 [TX-3] lowTransaction get end. 1[ms], transactionId=TID-0000001400002507
```

DB側で採番されたトランザクションIDが取得できると、それが出力される。

大抵の場合、トランザクション内で初めて実行するSQLの開始直後に出力される。
（トランザクション内で何か処理を行わないと、トランザクションIDを取得しない為）

#### select結果

```
08:55:05.381 [sql-5][ss-5] readCount=1, hasNextRow=true
```

実行したSQLがselect文の場合、DBからの読み込み終了時に、読み込んだ件数と「全レコードを読み終わったかどうか」が出力される。

hasNextRowは、基本的にはTsubakuroのResultSet#nextRow()の値である。
TsurugiTransactionのexecuteAndGetListメソッドの場合、全件読んで、hasNextRowはfalseになる。
executeAndFindRecordの場合、1件だけ読んで、hasNextRowはtrueになる（実際に次のレコードが有るか無いかに関わらず）。
1件も読んでいない場合、`readCount=0, hasNextRow=unread` になる。（ここだけがTsubakuroと異なる）
空のテーブルを読んだ場合、`readCount=0, hasNextRow=false` になる。

#### SQL実行終了

```
08:55:05.286 [sql-3][ss-3] sql end. 487[ms]
08:55:05.286 [sql-3][ss-3] sql close. close.elapsed=0[ms]
08:55:05.288 [TX-3][iceaxeTxExecuteId=3] executeAndGetCount(sql-3) end. 490[ms]
```

SQLの実行終了時のログ。

endの後ろの時間は、SQL実行の経過時間。
closeの後ろの時間は、クローズ処理の実行時間。
executeXxxの後ろの時間は、TsurugiTransaction#executeXxxメソッドの実行時間。

「TX-N」の行において、実行した「sql-N」の番号が出力されている。
TsurugiTransactionのexecuteXxxメソッド（上記の例ではexecuteAndGetCount）を開始した時点ではTsurugiSql側のID（sql-N）は採番されていなかったが、終了時点ではどのSQLを実行したか分かっている為。

TsurugiTransactionのexecuteQuery()とexecuteStatement()では、そのメソッドからSQL実行結果（sql-N）を返すので、executeQuery()やexecuteStatement()が終わった後にsql-Nが終了する。
その他のexecuteXxxメソッド（executeAndGetListやexecuteAndGetCount等）では、sql-Nが終わった後にexecuteXxxが終了する。

#### コミット・ロールバック

```
08:55:05.388 [TX-3] commit start. commitType=DEFAULT
08:55:05.391 [TX-3] commit end. 2[ms]
```

コミットやロールバックの経過時間が出力される。

#### トランザクションマネージャーの例外

```
09:28:06.837 [TM-6] tm.execute error
com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException: ERR_ABORTED_RETRYABLE: SQL--0011: 
```

TsurugiTransactionManager.execute()に渡されたアプリケーションの処理（関数）で例外が発生すると、それが出力される。

`[TM-N]`のNは、iceaxeTmExecuteId（前述の『TsurugiTransaction#toString()』を参照）。

#### トランザクションのリトライ発生

```
09:28:06.898 [TM-6] tm.execute(iceaxeTmExecuteId=6, attempt=0) retry. nextTx=OCC{}
```

シリアライゼイションエラー（リトライ可能なアボート）が発生してリトライする時、次のトランザクションのトランザクションオプションが出力される。

次のトランザクションは、iceaxeTmExecuteIdが同一で、attemptが1増えたものになる。
（リトライすることが決まった時点では、まだ次のトランザクションインスタンスは生成されていないので、トランザクション番号は出せない）

#### トランザクションの終了（クローズ）

```
08:55:05.391 [TX-3] transaction close. transaction.elapsed=597[ms]
```

トランザクションの経過時間が出力される。

#### 例外について

例外をログに出力するときは、スタックトレースが出力される。

ただし、全く同じ例外を複数回出力する場合は、2回目以降は例外クラス名と例外メッセージだけ出力される。
（ある例外がログに出力された後に、その例外が上位層にスローされた場合、上位層のイベントとして再度同じ例外がログに出力される可能性がある）



## 実行計画のJSONファイルの変換

SQLの実行計画のJSONファイルは、SQLコンソール（Tanzawa）の`--explain`オプションで、人間が見やすい形式に変換できる。

`--explain`オプションは隠し機能なので、ヘルプには出てこない。
`--explain`に関するオプションは以下の通り。

```bash
  Options:
  * --input, -i
      explain json file
    --output, -o
      output file (dot)
    --report, -r
      report to stdout
    --verbose, -v
      verbose
    -D
      client variable. <key>=<value>
      Syntax: -Dkey=value
      Default: {}
```

- `--input` JSONファイル
  - 実行計画のJSONファイル
- `--report`
  - SQLコンソールの（オプション無しの）explain文で表示されるのと同等のものを標準出力に出力する。
- `--output` 出力ファイル
  - SQLコンソールのdot.output付きのexplain文で出力されるのと同等のものを出力する。
    - 拡張子がdotの場合、Graphvizのdotファイルを出力する。
    - 拡張子がpdfだとpdfファイルを出力するが、その場合は別途dotコマンドのパスが設定されている必要がある。
- `--verbose`
  - 実行計画の出力内容に詳細な情報を追加する。
- `-D` key=value
  - Graphvizのdotコマンドに渡す引数
    - SQLコンソール（`console`サブコマンド）で指定するものと同じ。
  - `dot.executable`=パス
    - Graphvizのdotコマンドのパス
  - `dot.graph.randir=TB`
    - rankdir（グラフの向き）をTB（Top→Bottom）にする

`--report` と `--output` のどちらも指定しない場合は、何も出力されない。

### 例

```bash
$ tgsql --explain -i sql-15826.explain.json --report
1. scan (scan) {source: table, table: item_construction_master, access: range-scan}
2. group (group_exchange) {whole: true, sorted: true}
3. emit (emit)
```

