# PostgreSQL CDC

This provides an Akka Stream Source that can stream changes from a PostgreSQL database. Here, by
"change", we mean database events such as: RowDeleted(..), RowInserted(..), RowUpdated(..). A
typical practical use case  is to have a stream that continuously replicates data from PostgreSQL to ElasticSearch. 
Another use case is to stream changes from the database in order to display the changes 
as notifications in a Web UI. But more generally, this provides the tooling for implementing the [Strangler Application](https://www.martinfowler.com/bliki/StranglerApplication.html) /
[Event Interception](https://www.martinfowler.com/bliki/EventInterception.html) patterns that Martin Fowler popularized.

## How It Works

This Akka Stream Source makes use of the "logical decoding" feature of PostgreSQL (available since PostgreSQL 9.4).
It uses the `test_decoding` plugin that comes pre-packaged with PostgreSQL. Enabling a logical replication slot
in PostgreSQL requires very little configuration and, generally, has minimal performance overhead. However, please consult
the PostgreSQL documentation to understand the performance implications: [the PostgreSQL documentation](https://www.postgresql.org/docs/10.4/static/logicaldecoding-example.html).
**Note**: support for other decoding plugins (e.g., `wal2json` is in the works).

## Events Emitted

This Akka Streams Source emits elements of the type [ChangeSet](). A change set is a set of changes that share a
transaction id. A [Change]() can be one of the following:

* [RowInserted]()
* [RowUpdated]()
* [RowDeleted]()

## Artifacts

The PostgreSQL JDBC driver is included in the main JAR.

TODO: publish artifact.

### Reported Issues
[Github]()


## Usage

### PostgreSQL Configuration (Required)

According to the PostgreSQL documentation, before you can use logical decoding, you must set `wal_level` to `logical` and
`max_replication_slots` to at least 1. This can be as simple as:

```bash
echo "wal_level=logical" >> /etc/postgresql/10/main/postgresql.conf
echo "max_replication_slots=8" >> /etc/postgresql/10/main/postgresql.conf
```

If you use a cloud-based managed database service (e.g., AWS RDS), you usually don't have direct access to the `postgresql.conf` configuration file and you have to use
a configuration panel instead:

* On AWS RDS:
    * Use option groups: you simply have to set the ```rds.logical_replication``` parameter to ```1``` in the option group associated with your RDS instance. See [the AWS RDS documentation](https://aws.amazon.com/blogs/aws/amazon-rds-for-postgresql-new-minor-versions-logical-replication-dms-and-more/).

### Source Settings

We configure the source using [PostgreSQLInstance]() and [PgCdcSourceSettings]():

[PostgreSQLInstance]():

|Setting                 | Meaning                           | Required|
| -----------------------|-----------------------------------|---------|
| HikariCP Data Source   | A data source from where we can get a connection | yes     |
| slot name              | name of logical replication slot  | yes     |

[PgCdcSourceSettings]():

| Setting           | Meaning                                       | Default      | Required |
| ------------------|-----------------------------------------------|--------------|----------|
| createSlotOnStart | create the logical replication slot on start  | true         | no       |
| dropSlotOnFinish  | drop the logical replication slot on stop     | false        | no       |
| plugin            | only one option as of now                     | TestDecoding | no       |
| columnsToIgnore   | what columns to ignore per table. * syntax is supported: use it to ignore all columns in a table, or all columns with a given name regardless of what table they are in | none | no |
| mode              | choose between "Get" (at most once) or  "Peek" (at least once). If you choose "Peek", you'll need the [Ack Sink](## Ack Sink Settings), to acknowledge consumption of the event  | Get | no |
| maxItems          | maximum number of changes to pull in one go   | 128          | no           |
| pollInterval      | duration between polls                        | 2 seconds    | no           |

Example source settings:


Scala

```
 
val postgreSQLInstance = PostgreSQLInstance(hikariDataSource, "slot_name")

val changeDataCaptureSettings = PgCdcSourceSettings()
  .withCreateSlotOnStart(false)
  .withMaxItems(256)
  .withPollInterval(7.seconds)
  .withMode(Modes.Peek)
  .withColumnsToIgnore(Map("images" -> List("binary"), "user_personal_information" -> List("*")))
  // ignore the binary column in the images table and ignore the user_personal_information table
```

### Source Usage

Without further ado, a minimalist example:

```
implicit val system: ActorSystem = ActorSystem()
  
val slotName = "slot_name"

ChangeDataCapture
  .source(PostgreSQLInstance(hikariDataSource, slotName), PgCdcSourceSettings())
  .log("postgresqlcdc", cs ⇒ s"captured changes: ${cs.toString}")
  .withAttributes(Attributes.logLevels(onElement = Logging.InfoLevel))
  .to(Sink.ignore)
  .run()

```

### Ack Sink Settings

We configure the Ack sink using [PostgreSQLInstance]()

[PostgreSQLInstance]()

|Setting                 | Meaning                           | Required|
| -----------------------|-----------------------------------|---------|
| HikariCP Data Source | A data source from where we can get a connection | yes     |
| slot name              | name of logical replication slot  | yes     |

[PgAckSinkSettings]()

|Setting               |Meaning                                                                     | Default              | Required |
|----------------------|----------------------------------------------------------------------------|----------------------|----------|
| maxItems             | ideal number of items to acknowledge at once                               | 16                   | no       |
| maxItemsWait         | maximum duration for which the stage waits until maxItems have accumulated | 3 second             | no       |


### Ack Sink Usage

You want to map these database change events (i.e. RowInserted) to domain events
(i.e. UserRegistered - aiming to adopt a Domain Driven Design approach) and publish the domain events to a queue (e.g, AWS SQS)
and use the Ack Sink to acknowledge consumption.

```

// Define your domain event
case class UserRegistered(id: String)

val slotName = "slot_name"

val pgSqlInstance = PostgreSQLInstance(hikariDataSource, slotName)
val source = ChangeDataCapture.source(pgSqlInstance, PgCdcSourceSettings())
val ackSink = ChangeDataCapture.ackSink(pgSqlInstance, PgCdcAckSinkSettings())

source
  .mapConcat(_.changes)
  .collect { // collect is map and filter
    case change @ RowInserted("public", "users", _, _, data) ⇒
      val userId = data("user_id")
      (change, UserRegistered(userId))
  }
  .map(identity) // do something useful e.g., publish to SQS
  .map(t ⇒ AckLogSeqNum(t._1.commitLogSeqNum))
  .to(ackSink) // acknowledge
  .run()

```


## Limitations

* It cannot capture events regarding changes to tables' structure (i.e. column removed, table dropped, table
index added, change of column type etc.).
* When a row update is captured, the previous version of the row is lost / not available.
    * Unless the table has a non-default `REPLICA IDENTITY` setting. To get the full previous version of the row: `ALTER TABLE "TABLE_NAME" REPLICA IDENTITY FULL`.
* It doesn't work with older version of PostgreSQL (i.e < 9.4).

## Important Note

Remember to drop the slot when you decide you no longer need the change data capture pipeline
```
SELECT * FROM pg_drop_replication_slot('name')
```
