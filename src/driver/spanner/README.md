### caution about spanner transaction
- recommended to use [Connection/EntityManager.transaction](https://github.com/typeorm/typeorm/blob/master/docs/transactions.md#creating-and-using-transactions) and write transactional operation inside callback, and make it non-side-effect except to ones for database. because spanner abort transaction operation when anomaly detected, and require caller to restart it. with using this callback style, the restart happens automatically, and transaction fails only un-recoverable error raises.
- [QueryRunner style](https://github.com/typeorm/typeorm/blob/master/docs/transactions.md#using-queryrunner-to-create-and-control-state-of-single-database-connection) can be used. but you should keep in mind that there is no automatic restart when spanner abort transaction, so you need to handle restarting by yourself. failure rate maybe much higher than you expected when you try to write columns which write concurrency is expected to be high.

### caution about spanner column options
- only ```NOT NULL ``` and  ```allow_commit_timestamp = { true | null } ``` supported. 