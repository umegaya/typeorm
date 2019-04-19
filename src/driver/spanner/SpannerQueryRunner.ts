import {QueryRunner} from "../../query-runner/QueryRunner";
import {TransactionAlreadyStartedError} from "../../error/TransactionAlreadyStartedError";
import {TransactionNotStartedError} from "../../error/TransactionNotStartedError";
import {TableColumn} from "../../schema-builder/table/TableColumn";
import {Table} from "../../schema-builder/table/Table";
import {TableForeignKey} from "../../schema-builder/table/TableForeignKey";
import {TableIndex} from "../../schema-builder/table/TableIndex";
import {QueryRunnerAlreadyReleasedError} from "../../error/QueryRunnerAlreadyReleasedError";
import {SpannerDriver, SpannerColumnUpdateWithCommitTimestamp} from "./SpannerDriver";
import {
    SpannerExtendSchemas,
    SpannerExtendColumnSchema,
    SpannerExtendedColumnProps,
    SpannerExtendedTableProps,
    SpannerExtendedColumnPropsFromTableColumn,
    SpannerExtendSchemaSources
} from "./SpannerRawTypes";
import {SpannerDDLTransformer} from "./SpannerDDLTransformer";
import {ReadStream} from "../../platform/PlatformTools";
import {EntityMetadata} from "../../metadata/EntityMetadata";
import {RandomGenerator} from "../../util/RandomGenerator";
import {QueryFailedError} from "../../error/QueryFailedError";
import {TableUnique} from "../../schema-builder/table/TableUnique";
import {BaseQueryRunner} from "../../query-runner/BaseQueryRunner";
import {Broadcaster} from "../../subscriber/Broadcaster";
import {PromiseUtils} from "../../index";
import {TableCheck} from "../../schema-builder/table/TableCheck";
import {IsolationLevel} from "../types/IsolationLevel";
import {EntityManager} from "../../entity-manager/EntityManager";
import {QueryBuilder} from "../../query-builder/QueryBuilder";
import {ObjectLiteral} from "../../common/ObjectLiteral";
import {TableExclusion} from "../../schema-builder/table/TableExclusion";

/**
 * Runs queries on a single mysql database connection.
 */
export class SpannerQueryRunner extends BaseQueryRunner implements QueryRunner {

    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Database driver used by connection.
     */
    driver: SpannerDriver;

    // -------------------------------------------------------------------------
    // Protected Properties
    // -------------------------------------------------------------------------

    /**
     * transaction if startsTransaction
     */
    protected tx: any;

    /**
     * disable ddl parser. from synchronization, actual spanner DDL generated,
     * so no need to parse even if option specify to use it.
     */
    protected disableDDLParser: boolean;


    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(driver: SpannerDriver) {
        super();
        this.driver = driver;
        this.disableDDLParser = false;
        this.connection = driver.connection;
        this.broadcaster = new Broadcaster(this);
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Creates/uses database connection from the connection pool to perform further operations.
     * Returns obtained database connection.
     */
    connect(): Promise<any> {
        if (!this.databaseConnection) {
            return (async () => {
                this.databaseConnection = await this.driver.getDatabaseHandle();
            })();
        }
        return Promise.resolve(this.databaseConnection);
    }

    /**
     * Releases used database connection.
     * You cannot use query runner methods once its released.
     */
    release(): Promise<void> {
        return Promise.resolve();
    }

    /**
     * Starts transaction on the current connection.
     */
    async startTransaction(isolationLevel?: IsolationLevel): Promise<void> {
        if (!this.driver.enableTransaction) {
            //console.log('startTransaction(ignored)');
            return Promise.resolve();
        }
        //console.log('startTransaction');
        if (this.isTransactionActive)
            throw new TransactionAlreadyStartedError();

        this.isTransactionActive = true;
        return this.connect().then(async (db) => {
            this.tx = (await this.databaseConnection.getTransaction())[0];
        });
    }

    /**
     * Commits transaction.
     * Error will be thrown if transaction was not started.
     */
    commitTransaction(): Promise<void> {
        if (!this.driver.enableTransaction) {
            //console.log('commitTransaction(ignored)');
            return Promise.resolve();
        }
        //console.log('commitTransaction');
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        return new Promise((res, rej) => this.tx.commit((err: Error, _: any) => {
            //console.log('commitTransaction cb', err, _);
            if (err) { rej(err); }
            else {
                this.tx = null;
                this.isTransactionActive = false;
                res();
            }
        }));
    }

    /**
     * Rollbacks transaction.
     * Error will be thrown if transaction was not started.
     */
    async rollbackTransaction(): Promise<void> {
        if (!this.driver.enableTransaction) {
            //console.log('rollbackTransaction(ignored)');
            return Promise.resolve();
        }
        if (!this.isTransactionActive)
            throw new TransactionNotStartedError();

        await new Promise((res, rej) => this.tx.rollback((err: Error, _: any) => {
            if (err) { rej(err); }
            else {
                this.tx = null;
                this.isTransactionActive = false;
                res();
            }
        }));
    }

    /**
     * Run provided function in transaction.
     * internally it may use start/commit Transaction.
     * Error will be thrown if transaction start/commit will fails
     */
    async runInTransaction<T>(
        runInTransaction: (tx: EntityManager) => Promise<T>,
        isolationLevel?: IsolationLevel
    ): Promise<T> {
        return new Promise<T>((res, rej) => {
            this.connect().then(async (db) => {
                this.databaseConnection.runTransaction(
                async (err: Error, tx: any) => {
                    if (err) {
                        rej(err);
                        return;
                    }
                    this.tx = tx;
                    this.isTransactionActive = true;
                    try {
                        const r = await runInTransaction(this.manager);
                        tx.commit((err2: Error, _: any) => {
                            if (err2) {
                                rej(err2);
                            } else {
                                this.tx = null;
                                this.isTransactionActive = false;
                                res(r);
                            }
                        });
                    } catch (e) {
                        tx.rollback((_: Error) => {
                            rej(e);
                        });
                    }
                });
            });
        });
    }

    /**
     * Executes sql used special for schema build.
     */
    protected async executeQueries(upQueries: string|string[], downQueries: string|string[]): Promise<void> {
        this.disableDDLParser = true;
        await super.executeQueries(upQueries, downQueries);
        this.disableDDLParser = false;
    }

    /**
     * Executes a raw SQL query.
     */
    query(query: string, parameters?: any[]): Promise<any> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError();

        // handle administrative queries.
        let m: RegExpMatchArray | null;
        if ((m = query.match(/^\s*(CREATE|DROP|ALTER)\s+(.+)/))) {
            return this.handleAdministrativeQuery(m[1], m);
        } else if (!query.match(/^\s*SELECT\s+(.+)/)) {
            throw new Error(`the query cannot handle by this function: ${query}`);
        }

        return new Promise(async (ok, fail) => {
            try {
                await this.connect();
                const db = this.tx || this.databaseConnection;
                parameters = parameters || [];
                const [ params, types ] = parameters;
                //console.log('query', query, params, types);
                this.driver.connection.logger.logQuery(query, params, this);
                const queryStartTime = +new Date();
                db.run({sql: query, params, types, json:true}, (err: any, result: any) => {
                    // log slow queries if maxQueryExecution time is set
                    const maxQueryExecutionTime = this.driver.connection.options.maxQueryExecutionTime;
                    const queryEndTime = +new Date();
                    const queryExecutionTime = queryEndTime - queryStartTime;
                    //console.log('query time', queryExecutionTime, 'ms');
                    if (maxQueryExecutionTime && queryExecutionTime > maxQueryExecutionTime)
                        this.driver.connection.logger.logQuerySlow(queryExecutionTime, query, parameters, this);

                    if (err) {
                        this.driver.connection.logger.logQueryError(err, query, parameters, this);
                        fail(new QueryFailedError(query, parameters, err));
                        return;
                    }

                    //console.log('query()', result);
                    ok(result);
                });

            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * execute query. call from XXXQueryBuilder
     */
    queryByBuilder<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError();

        const fmaps: { [key:string]:(qb:QueryBuilder<Entity>) => Promise<any>} = {
            select: this.select,
            insert: this.insert,
            update: this.update,
            delete: this.delete
        };

        return this.connect().then(() => {
            return fmaps[qb.expressionMap.queryType].call(this, qb);
        });
    }

    queryByBuilderAndParams<Entity>(qb: QueryBuilder<Entity>, sql:string, params?:any[]): Promise<any> {
        return this.query(sql, params);
    }

    /**
     * Returns raw data stream.
     */
    stream(query: string, parameters?: any[], onEnd?: Function, onError?: Function): Promise<ReadStream> {
        if (this.isReleased)
            throw new QueryRunnerAlreadyReleasedError();

        return new Promise(async (ok, fail) => {
            try {
                await this.connect();
                const db = this.databaseConnection;
                this.driver.connection.logger.logQuery(query, parameters, this);
                parameters = parameters || [];
                const [ params, types ] = parameters;
                const stream = db.runStream({sql: query, params, types});
                if (onEnd) stream.on("end", onEnd);
                if (onError) stream.on("error", onError);
                ok(stream);

            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * Returns all available database names including system databases.
     */
    async getDatabases(): Promise<string[]> {
        return this.driver.getDatabases();
    }

    /**
     * Returns all available schema names including system schemas.
     * If database parameter specified, returns schemas of that database.
     */
    async getSchemas(database?: string): Promise<string[]> {
        throw new Error(`NYI: spanner: getSchemas`);
    }

    /**
     * Checks if database with the given name exist.
     */
    async hasDatabase(database: string): Promise<boolean> {
        return this.connect().then(async () => {
            const dbs = await this.driver.getDatabases();
            return dbs.indexOf(database) >= 0;
        });
    }

    /**
     * Checks if schema with the given name exist.
     */
    async hasSchema(schema: string): Promise<boolean> {
        throw new Error(`NYI: spanner: hasSchema`);
    }

    /**
     * Checks if table with the given name exist in the database.
     */
    async hasTable(tableOrName: Table|string): Promise<boolean> {
        return this.connect().then(async () => {
            const table = await this.driver.loadTables(tableOrName);
            //console.log('hasTable', tableOrName, !!table[0]);
            return !!table[0];
        });
    }

    /**
     * Checks if column with the given name exist in the given table.
     */
    async hasColumn(tableOrName: Table|string, column: TableColumn|string): Promise<boolean> {
        return this.connect().then(async () => {
            const tables = await this.driver.loadTables(tableOrName);
            return !!tables[0].columns.find((c: TableColumn) => {
                if (typeof column === 'string' && c.name == column) {
                    return true;
                } else if (column instanceof TableColumn && c.name == column.name) {
                    return true;
                }
                return false;
            });
        });
    }

    /**
     * Creates a new database.
     */
    async createDatabase(database: string, ifNotExist?: boolean): Promise<void> {
        const up = ifNotExist ? `CREATE DATABASE IF NOT EXISTS \`${database}\`` : `CREATE DATABASE \`${database}\``;
        const down = `DROP DATABASE \`${database}\``;
        await this.executeQueries(up, down);
    }

    /**
     * Drops database.
     */
    async dropDatabase(database: string, ifExist?: boolean): Promise<void> {
        const up = ifExist ? `DROP DATABASE IF EXISTS \`${database}\`` : `DROP DATABASE \`${database}\``;
        const down = `CREATE DATABASE \`${database}\``;
        await this.executeQueries(up, down);
    }

    /**
     * Creates a new table schema.
     */
    async createSchema(schema: string, ifNotExist?: boolean): Promise<void> {
        throw new Error(`NYI: spanner: createSchema`);
    }

    /**
     * Drops table schema.
     */
    async dropSchema(schemaPath: string, ifExist?: boolean): Promise<void> {
        throw new Error(`NYI: spanner: dropSchema`);
    }

    /**
     * Creates a new table. aka 'schema' on spanner
     * note that foreign key always dropped regardless the value of createForeignKeys.
     * because our foreignkey analogue is achieved by interleaved table
     */
    async createTable(table: Table, ifNotExist: boolean = false, createForeignKeys: boolean = true): Promise<void> {
        if (ifNotExist) {
            const isTableExist = await this.hasTable(table);
            if (isTableExist) return Promise.resolve();
        }
        const upQueries: string[] = [];
        const downQueries: string[] = [];

        // create table sql.
        upQueries.push(this.createTableSql(table));
        downQueries.push(this.dropTableSql(table));

        console.log('createTable', `sql=[${upQueries[0]}]`);

        // create indexes. unique constraint will be integrated with unique index.
        if (table.uniques.length > 0) {
            table.uniques.forEach(unique => {
                const uniqueExist = table.indices.some(index => index.name === unique.name);
                if (!uniqueExist) {
                    table.indices.push(new TableIndex({
                        name: unique.name,
                        columnNames: unique.columnNames,
                        isUnique: true
                    }));
                }
            });
        }

        if (table.indices.length > 0) {
            table.indices.forEach(index => {
                upQueries.push(this.createIndexSql(table, index));
                downQueries.push(this.dropIndexSql(table, index));
            });
        }

        // we don't drop foreign key itself. because its created with table
        // if (createForeignKeys)
        // table.foreignKeys.forEach(foreignKey => downQueries.push(this.dropForeignKeySql(table, foreignKey)));

        await this.executeQueries(upQueries, downQueries);

        // super.replaceCachedTable will be ignored because new table should not be loaded before.
        this.replaceCachedTable(table, table);

    }

    /**
     * Drop the table.
     * note that foreign key always dropped regardless the value of dropForeignKeys.
     * because our foreignkey analogue is achieved by interleaved table
     */
    async dropTable(target: Table|string, ifExist?: boolean, dropForeignKeys: boolean = true): Promise<void> {
        // It needs because if table does not exist and dropForeignKeys or dropIndices is true, we don't need
        // to perform drop queries for foreign keys and indices.
        if (ifExist) {
            const isTableExist = await this.hasTable(target);
            if (!isTableExist) return Promise.resolve();
        }

        // if dropTable called with dropForeignKeys = true, we must create foreign keys in down query.
        // const createForeignKeys: boolean = dropForeignKeys;
        const tableName = target instanceof Table ? target.name : target;
        const table = await this.getCachedTable(tableName);
        const upQueries: string[] = [];
        const downQueries: string[] = [];

        // if (dropForeignKeys)
        // table.foreignKeys.forEach(foreignKey => upQueries.push(this.dropForeignKeySql(table, foreignKey)));

        if (table.indices.length > 0) {
            table.indices.forEach(index => {
                upQueries.push(this.dropIndexSql(table, index))
                downQueries.push(this.createIndexSql(table, index))
            });
        }

        upQueries.push(this.dropTableSql(table));
        downQueries.push(this.createTableSql(table));

        await this.executeQueries(upQueries, downQueries);

        // remove table from cache
        this.replaceCachedTable(table, null);
    }

    /**
     * Renames a table.
     */
    async renameTable(oldTableOrName: Table|string, newTableName: string): Promise<void> {
        // TODO: re-create table
        throw new Error(`NYI: spanner: renameTable`);

        /*const upQueries: string[] = [];
        const downQueries: string[] = [];
        const oldTable = oldTableOrName instanceof Table ? oldTableOrName : await this.getCachedTable(oldTableOrName);
        const newTable = oldTable.clone();
        const dbName = oldTable.name.indexOf(".") === -1 ? undefined : oldTable.name.split(".")[0];
        newTable.name = dbName ? `${dbName}.${newTableName}` : newTableName;

        // rename table
        upQueries.push(`RENAME TABLE ${this.escapeTableName(oldTable.name)} TO ${this.escapeTableName(newTable.name)}`);
        downQueries.push(`RENAME TABLE ${this.escapeTableName(newTable.name)} TO ${this.escapeTableName(oldTable.name)}`);

        // rename index constraints
        newTable.indices.forEach(index => {
            // build new constraint name
            const columnNames = index.columnNames.map(column => `\`${column}\``).join(", ");
            const newIndexName = this.connection.namingStrategy.indexName(newTable, index.columnNames, index.where);

            // build queries
            let indexType = "";
            if (index.isUnique)
                indexType += "UNIQUE ";
            if (index.isSpatial)
                indexType += "SPATIAL ";
            if (index.isFulltext)
                indexType += "FULLTEXT ";
            upQueries.push(`ALTER TABLE ${this.escapeTableName(newTable)} DROP INDEX \`${index.name}\`, ADD ${indexType}INDEX \`${newIndexName}\` (${columnNames})`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(newTable)} DROP INDEX \`${newIndexName}\`, ADD ${indexType}INDEX \`${index.name}\` (${columnNames})`);

            // replace constraint name
            index.name = newIndexName;
        });

        // rename foreign key constraint
        newTable.foreignKeys.forEach(foreignKey => {
            // build new constraint name
            const columnNames = foreignKey.columnNames.map(column => `\`${column}\``).join(", ");
            const referencedColumnNames = foreignKey.referencedColumnNames.map(column => `\`${column}\``).join(",");
            const newForeignKeyName = this.connection.namingStrategy.foreignKeyName(newTable, foreignKey.columnNames);

            // build queries
            let up = `ALTER TABLE ${this.escapeTableName(newTable)} DROP FOREIGN KEY \`${foreignKey.name}\`, ADD CONSTRAINT \`${newForeignKeyName}\` FOREIGN KEY (${columnNames}) ` +
                `REFERENCES ${this.escapeTableName(foreignKey.referencedTableName)}(${referencedColumnNames})`;
            if (foreignKey.onDelete)
                up += ` ON DELETE ${foreignKey.onDelete}`;
            if (foreignKey.onUpdate)
                up += ` ON UPDATE ${foreignKey.onUpdate}`;

            let down = `ALTER TABLE ${this.escapeTableName(newTable)} DROP FOREIGN KEY \`${newForeignKeyName}\`, ADD CONSTRAINT \`${foreignKey.name}\` FOREIGN KEY (${columnNames}) ` +
                `REFERENCES ${this.escapeTableName(foreignKey.referencedTableName)}(${referencedColumnNames})`;
            if (foreignKey.onDelete)
                down += ` ON DELETE ${foreignKey.onDelete}`;
            if (foreignKey.onUpdate)
                down += ` ON UPDATE ${foreignKey.onUpdate}`;

            upQueries.push(up);
            downQueries.push(down);

            // replace constraint name
            foreignKey.name = newForeignKeyName;
        });

        await this.executeQueries(upQueries, downQueries);

        // rename old table and replace it in cached tabled;
        oldTable.name = newTable.name;
        this.replaceCachedTable(oldTable, newTable);*/
    }

    /**
     * Creates a new column from the column in the table.
     */
    async addColumn(tableOrName: Table|string, column: TableColumn): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();
        const upQueries: string[] = [];
        const downQueries: string[] = [];
        const skipColumnLevelPrimary = clonedTable.primaryColumns.length > 0;

        upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD ${this.buildCreateColumnSql(column, skipColumnLevelPrimary, false)}`);
        downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP COLUMN \`${column.name}\``);

        // create or update primary key constraint
        if (column.isPrimary) {
            // TODO: re-create table
            throw new Error(`NYI: spanner: addColumn column.isPrimary`);
            /*
            // if we already have generated column, we must temporary drop AUTO_INCREMENT property.
            const generatedColumn = clonedTable.columns.find(column => column.isGenerated && column.generationStrategy === "increment");
            if (generatedColumn) {
                const nonGeneratedColumn = generatedColumn.clone();
                nonGeneratedColumn.isGenerated = false;
                nonGeneratedColumn.generationStrategy = undefined;
                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${column.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(column, true)}`);
            }

            const primaryColumns = clonedTable.primaryColumns;
            let columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);

            primaryColumns.push(column);
            columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);

            // if we previously dropped AUTO_INCREMENT property, we must bring it back
            if (generatedColumn) {
                const nonGeneratedColumn = generatedColumn.clone();
                nonGeneratedColumn.isGenerated = false;
                nonGeneratedColumn.generationStrategy = undefined;
                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(column, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${column.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
            }
            */
        }

        // create column index
        const columnIndex = clonedTable.indices.find(index => index.columnNames.length === 1 && index.columnNames[0] === column.name);
        if (columnIndex) {
            upQueries.push(this.createIndexSql(table, columnIndex));
            downQueries.push(this.dropIndexSql(table, columnIndex));

        } else if (column.isUnique) {
            const uniqueIndex = new TableIndex({
                name: this.connection.namingStrategy.indexName(table.name, [column.name]),
                columnNames: [column.name],
                isUnique: true
            });
            clonedTable.indices.push(uniqueIndex);
            clonedTable.uniques.push(new TableUnique({
                name: uniqueIndex.name,
                columnNames: uniqueIndex.columnNames
            }));
            upQueries.push(this.createIndexSql(table, uniqueIndex));
            downQueries.push(this.dropIndexSql(table, uniqueIndex));
        }

        await this.executeQueries(upQueries, downQueries);

        clonedTable.addColumn(column);
        this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Creates a new columns from the column in the table.
     */
    async addColumns(tableOrName: Table|string, columns: TableColumn[]): Promise<void> {
        await PromiseUtils.runInSequence(columns, column => this.addColumn(tableOrName, column));
    }

    /**
     * Renames column in the given table.
     */
    async renameColumn(tableOrName: Table|string, oldTableColumnOrName: TableColumn|string, newTableColumnOrName: TableColumn|string): Promise<void> {
        throw new Error(`NYI: spanner: renameColumn. you can remove column first, then create with the same name.`);
        /*
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const oldColumn = oldTableColumnOrName instanceof TableColumn ? oldTableColumnOrName : table.columns.find(c => c.name === oldTableColumnOrName);
        if (!oldColumn)
            throw new Error(`Column "${oldTableColumnOrName}" was not found in the "${table.name}" table.`);

        let newColumn: TableColumn|undefined = undefined;
        if (newTableColumnOrName instanceof TableColumn) {
            newColumn = newTableColumnOrName;
        } else {
            newColumn = oldColumn.clone();
            newColumn.name = newTableColumnOrName;
        }

        await this.changeColumn(table, oldColumn, newColumn);
        */
    }

    /**
     * Changes a column in the table.
     * according to https://cloud.google.com/spanner/docs/schema-updates, only below are allowed
     * - Change a STRING column to a BYTES column or a BYTES column to a STRING column.
     * - Increase or decrease the length limit for a STRING or BYTES type (including to MAX), unless it is a primary key column inherited by one or more child tables.
     * - Add/Remove NOT NULL constraint for non-key column
     * - Enable or disable commit timestamps in value and primary key columns.
     */
    async changeColumn(tableOrName: Table|string, oldColumnOrName: TableColumn|string, newColumn: TableColumn): Promise<void> {
        //TODO: implement above changes in comment

        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        let clonedTable = table.clone();
        const upQueries: string[] = [];
        const downQueries: string[] = [];

        const oldColumn = oldColumnOrName instanceof TableColumn
            ? oldColumnOrName
            : table.columns.find(column => column.name === oldColumnOrName);
        if (!oldColumn)
            throw new Error(`Column "${oldColumnOrName}" was not found in the "${table.name}" table.`);

        if (oldColumn.name !== newColumn.name) {
            throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: change column name ${oldColumn.name} => ${newColumn.name}`);
        }

        if (oldColumn.type !== newColumn.type) {
            // - Change a STRING column to a BYTES column or a BYTES column to a STRING column.
            if (!(oldColumn.type === "string" && newColumn.type === "bytes") &&
                !(oldColumn.type === "bytes" && newColumn.type === "string")) {
                throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: change column type ${oldColumn.type} => ${newColumn.type}`);
            }
        }

        if (oldColumn.length && newColumn.length && (oldColumn.length !== newColumn.length)) {
            // - Increase or decrease the length limit for a STRING or BYTES type (including to MAX)
            if (!(oldColumn.type === "string" && newColumn.type === "bytes") &&
                !(oldColumn.type === "bytes" && newColumn.type === "string")) {
                throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: change column type ${oldColumn.type} => ${newColumn.type}`);
            }
            // TODO: implement following check.
            // `unless it is a primary key column inherited by one or more child tables.`
        }

        if (oldColumn.isNullable !== newColumn.isNullable) {
            // - Add/Remove NOT NULL constraint for non-key column
            if (clonedTable.indices.find(index => {
                return index.columnNames.length === 1 && index.columnNames[0] === newColumn.name;
            })) {
                throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: change nullable for ${oldColumn.name}, which is indexed`);
            }
        }

        // - Enable or disable commit timestamps in value and primary key columns.
        if (oldColumn.default !== newColumn.default) {
            if (newColumn.default !== SpannerColumnUpdateWithCommitTimestamp &&
                oldColumn.default !== SpannerColumnUpdateWithCommitTimestamp) {
                console.log("oldColumn:" + JSON.stringify(oldColumn));
                throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: set default ${oldColumn.default} => ${newColumn.default}`);

            }
        }

        // any other invalid changes
        if (oldColumn.isPrimary !== newColumn.isPrimary ||
            oldColumn.asExpression !== newColumn.asExpression ||
            oldColumn.charset !== newColumn.charset ||
            oldColumn.collation !== newColumn.collation ||
            // comment is not supported by spanner
            // default is managed by schemas table.
            oldColumn.enum !== newColumn.enum ||
            oldColumn.generatedType !== newColumn.generatedType ||
            // generationStorategy is managed by schemas table
            oldColumn.isArray !== newColumn.isArray
            // isGenerated is managed by schemas table
        ) {
            throw new Error(`NYI: spanner: changeColumn ${oldColumn.name}: not supported change ${JSON.stringify(oldColumn)} => ${JSON.stringify(newColumn)}`);
        }

        // if actually changed, store SQLs
        if (this.isColumnChanged(oldColumn, newColumn, true)) {
            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ALTER COLUMN \`${oldColumn.name}\` ${this.buildCreateColumnSql(newColumn, true)}`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ALTER COLUMN \`${newColumn.name}\` ${this.buildCreateColumnSql(oldColumn, true)}`);
        }

        await this.executeQueries(upQueries, downQueries);
        this.replaceCachedTable(table, clonedTable);


        /*
        if ((newColumn.isGenerated !== oldColumn.isGenerated && newColumn.generationStrategy !== "uuid")
            || oldColumn.type !== newColumn.type
            || oldColumn.length !== newColumn.length
            || oldColumn.generatedType !== newColumn.generatedType) {
            await this.dropColumn(table, oldColumn);
            await this.addColumn(table, newColumn);

            // update cloned table
            clonedTable = table.clone();

        } else {
            if (newColumn.name !== oldColumn.name) {
                // We don't change any column properties, just rename it.
                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${oldColumn.name}\` \`${newColumn.name}\` ${this.buildCreateColumnSql(oldColumn, true, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${newColumn.name}\` \`${oldColumn.name}\` ${this.buildCreateColumnSql(oldColumn, true, true)}`);

                // rename index constraints
                clonedTable.findColumnIndices(oldColumn).forEach(index => {
                    // build new constraint name
                    index.columnNames.splice(index.columnNames.indexOf(oldColumn.name), 1);
                    index.columnNames.push(newColumn.name);
                    const columnNames = index.columnNames.map(column => `\`${column}\``).join(", ");
                    const newIndexName = this.connection.namingStrategy.indexName(clonedTable, index.columnNames, index.where);

                    // build queries
                    let indexType = "";
                    if (index.isUnique)
                        indexType += "UNIQUE ";
                    if (index.isSpatial)
                        indexType += "SPATIAL ";
                    if (index.isFulltext)
                        indexType += "FULLTEXT ";
                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP INDEX \`${index.name}\`, ADD ${indexType}INDEX \`${newIndexName}\` (${columnNames})`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP INDEX \`${newIndexName}\`, ADD ${indexType}INDEX \`${index.name}\` (${columnNames})`);

                    // replace constraint name
                    index.name = newIndexName;
                });

                // rename foreign key constraints
                clonedTable.findColumnForeignKeys(oldColumn).forEach(foreignKey => {
                    // build new constraint name
                    foreignKey.columnNames.splice(foreignKey.columnNames.indexOf(oldColumn.name), 1);
                    foreignKey.columnNames.push(newColumn.name);
                    const columnNames = foreignKey.columnNames.map(column => `\`${column}\``).join(", ");
                    const referencedColumnNames = foreignKey.referencedColumnNames.map(column => `\`${column}\``).join(",");
                    const newForeignKeyName = this.connection.namingStrategy.foreignKeyName(clonedTable, foreignKey.columnNames);

                    // build queries
                    let up = `ALTER TABLE ${this.escapeTableName(table)} DROP FOREIGN KEY \`${foreignKey.name}\`, ADD CONSTRAINT \`${newForeignKeyName}\` FOREIGN KEY (${columnNames}) ` +
                        `REFERENCES ${this.escapeTableName(foreignKey.referencedTableName)}(${referencedColumnNames})`;
                    if (foreignKey.onDelete)
                        up += ` ON DELETE ${foreignKey.onDelete}`;
                    if (foreignKey.onUpdate)
                        up += ` ON UPDATE ${foreignKey.onUpdate}`;

                    let down = `ALTER TABLE ${this.escapeTableName(table)} DROP FOREIGN KEY \`${newForeignKeyName}\`, ADD CONSTRAINT \`${foreignKey.name}\` FOREIGN KEY (${columnNames}) ` +
                        `REFERENCES ${this.escapeTableName(foreignKey.referencedTableName)}(${referencedColumnNames})`;
                    if (foreignKey.onDelete)
                        down += ` ON DELETE ${foreignKey.onDelete}`;
                    if (foreignKey.onUpdate)
                        down += ` ON UPDATE ${foreignKey.onUpdate}`;

                    upQueries.push(up);
                    downQueries.push(down);

                    // replace constraint name
                    foreignKey.name = newForeignKeyName;
                });

                // rename old column in the Table object
                const oldTableColumn = clonedTable.columns.find(column => column.name === oldColumn.name);
                clonedTable.columns[clonedTable.columns.indexOf(oldTableColumn!)].name = newColumn.name;
                oldColumn.name = newColumn.name;
            }

            if (this.isColumnChanged(oldColumn, newColumn, true)) {
                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${oldColumn.name}\` ${this.buildCreateColumnSql(newColumn, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${newColumn.name}\` ${this.buildCreateColumnSql(oldColumn, true)}`);
            }

            if (newColumn.isPrimary !== oldColumn.isPrimary) {
                // if table have generated column, we must drop AUTO_INCREMENT before changing primary constraints.
                const generatedColumn = clonedTable.columns.find(column => column.isGenerated && column.generationStrategy === "increment");
                if (generatedColumn) {
                    const nonGeneratedColumn = generatedColumn.clone();
                    nonGeneratedColumn.isGenerated = false;
                    nonGeneratedColumn.generationStrategy = undefined;

                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${generatedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(generatedColumn, true)}`);
                }

                const primaryColumns = clonedTable.primaryColumns;

                // if primary column state changed, we must always drop existed constraint.
                if (primaryColumns.length > 0) {
                    const columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);
                }

                if (newColumn.isPrimary === true) {
                    primaryColumns.push(newColumn);
                    // update column in table
                    const column = clonedTable.columns.find(column => column.name === newColumn.name);
                    column!.isPrimary = true;
                    const columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);

                } else {
                    const primaryColumn = primaryColumns.find(c => c.name === newColumn.name);
                    primaryColumns.splice(primaryColumns.indexOf(primaryColumn!), 1);
                    // update column in table
                    const column = clonedTable.columns.find(column => column.name === newColumn.name);
                    column!.isPrimary = false;

                    // if we have another primary keys, we must recreate constraint.
                    if (primaryColumns.length > 0) {
                        const columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
                        upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);
                        downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);
                    }
                }

                // if we have generated column, and we dropped AUTO_INCREMENT property before, we must bring it back
                if (generatedColumn) {
                    const nonGeneratedColumn = generatedColumn.clone();
                    nonGeneratedColumn.isGenerated = false;
                    nonGeneratedColumn.generationStrategy = undefined;

                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(generatedColumn, true)}`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${generatedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
                }
            }

            if (newColumn.isUnique !== oldColumn.isUnique) {
                if (newColumn.isUnique === true) {
                    const uniqueIndex = new TableIndex({
                        name: this.connection.namingStrategy.indexName(table.name, [newColumn.name]),
                        columnNames: [newColumn.name],
                        isUnique: true
                    });
                    clonedTable.indices.push(uniqueIndex);
                    clonedTable.uniques.push(new TableUnique({
                        name: uniqueIndex.name,
                        columnNames: uniqueIndex.columnNames
                    }));
                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD UNIQUE INDEX \`${uniqueIndex.name}\` (\`${newColumn.name}\`)`);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP INDEX \`${uniqueIndex.name}\``);

                } else {
                    const uniqueIndex = clonedTable.indices.find(index => {
                        return index.columnNames.length === 1 && index.isUnique === true && !!index.columnNames.find(columnName => columnName === newColumn.name);
                    });
                    clonedTable.indices.splice(clonedTable.indices.indexOf(uniqueIndex!), 1);

                    const tableUnique = clonedTable.uniques.find(unique => unique.name === uniqueIndex!.name);
                    clonedTable.uniques.splice(clonedTable.uniques.indexOf(tableUnique!), 1);

                    upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP INDEX \`${uniqueIndex!.name}\``);
                    downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD UNIQUE INDEX \`${uniqueIndex!.name}\` (\`${newColumn.name}\`)`);
                }
            }
        } */

        // await this.executeQueries(upQueries, downQueries);
        // this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Changes a column in the table.
     */
    async changeColumns(tableOrName: Table|string, changedColumns: { newColumn: TableColumn, oldColumn: TableColumn }[]): Promise<void> {
        await PromiseUtils.runInSequence(changedColumns, changedColumn => this.changeColumn(tableOrName, changedColumn.oldColumn, changedColumn.newColumn));
    }

    /**
     * Drops column in the table.
     */
    async dropColumn(tableOrName: Table|string, columnOrName: TableColumn|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const column = columnOrName instanceof TableColumn ? columnOrName : table.findColumnByName(columnOrName);
        if (!column)
            throw new Error(`Column "${columnOrName}" was not found in table "${table.name}"`);

        const clonedTable = table.clone();
        const upQueries: string[] = [];
        const downQueries: string[] = [];

        // drop primary key constraint
        if (column.isPrimary) {
            throw new Error(`NYI: spanner: dropColumn column.isPrimary`);
            /*
            // if table have generated column, we must drop AUTO_INCREMENT before changing primary constraints.
            const generatedColumn = clonedTable.columns.find(column => column.isGenerated && column.generationStrategy === "increment");
            if (generatedColumn) {
                const nonGeneratedColumn = generatedColumn.clone();
                nonGeneratedColumn.isGenerated = false;
                nonGeneratedColumn.generationStrategy = undefined;

                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${generatedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(generatedColumn, true)}`);
            }

            // dropping primary key constraint
            const columnNames = clonedTable.primaryColumns.map(primaryColumn => `\`${primaryColumn.name}\``).join(", ");
            upQueries.push(`ALTER TABLE ${this.escapeTableName(clonedTable)} DROP PRIMARY KEY`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(clonedTable)} ADD PRIMARY KEY (${columnNames})`);

            // update column in table
            const tableColumn = clonedTable.findColumnByName(column.name);
            tableColumn!.isPrimary = false;

            // if primary key have multiple columns, we must recreate it without dropped column
            if (clonedTable.primaryColumns.length > 0) {
                const columnNames = clonedTable.primaryColumns.map(primaryColumn => `\`${primaryColumn.name}\``).join(", ");
                upQueries.push(`ALTER TABLE ${this.escapeTableName(clonedTable)} ADD PRIMARY KEY (${columnNames})`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(clonedTable)} DROP PRIMARY KEY`);
            }

            // if we have generated column, and we dropped AUTO_INCREMENT property before, and this column is not current dropping column, we must bring it back
            if (generatedColumn && generatedColumn.name !== column.name) {
                const nonGeneratedColumn = generatedColumn.clone();
                nonGeneratedColumn.isGenerated = false;
                nonGeneratedColumn.generationStrategy = undefined;

                upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(generatedColumn, true)}`);
                downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${generatedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
            }
            */
        }

        // drop column index
        const columnIndex = clonedTable.indices.find(index => index.columnNames.length === 1 && index.columnNames[0] === column.name);
        if (columnIndex) {
            clonedTable.indices.splice(clonedTable.indices.indexOf(columnIndex), 1);
            upQueries.push(this.dropIndexSql(table, columnIndex));
            downQueries.push(this.createIndexSql(table, columnIndex));

        } else if (column.isUnique) {
            // we splice constraints both from table uniques and indices.
            const uniqueName = this.connection.namingStrategy.uniqueConstraintName(table.name, [column.name]);
            const foundUnique = clonedTable.uniques.find(unique => unique.name === uniqueName);
            if (foundUnique)
                clonedTable.uniques.splice(clonedTable.uniques.indexOf(foundUnique), 1);

            const indexName = this.connection.namingStrategy.indexName(table.name, [column.name]);
            const foundIndex = clonedTable.indices.find(index => index.name === indexName);
            if (foundIndex) {
                clonedTable.indices.splice(clonedTable.indices.indexOf(foundIndex), 1);
                upQueries.push(this.dropIndexSql(table, foundIndex));
                downQueries.push(this.createIndexSql(table, foundIndex));
            }
        }

        upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP COLUMN \`${column.name}\``);
        downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD ${this.buildCreateColumnSql(column, true)}`);

        await this.executeQueries(upQueries, downQueries);

        clonedTable.removeColumn(column);
        this.replaceCachedTable(table, clonedTable);
    }

    /**
     * Drops the columns in the table.
     */
    async dropColumns(tableOrName: Table|string, columns: TableColumn[]): Promise<void> {
        await PromiseUtils.runInSequence(columns, column => this.dropColumn(tableOrName, column));
    }

    /**
     * Creates a new primary key.
     */
    async createPrimaryKey(tableOrName: Table|string, columnNames: string[]): Promise<void> {
        throw new Error(`NYI: spanner: createPrimaryKey`);

        /*
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();

        const up = this.createPrimaryKeySql(table, columnNames);
        const down = this.dropPrimaryKeySql(table);

        await this.executeQueries(up, down);
        clonedTable.columns.forEach(column => {
            if (columnNames.find(columnName => columnName === column.name))
                column.isPrimary = true;
        });
        this.replaceCachedTable(table, clonedTable);
        */
    }

    /**
     * Updates composite primary keys.
     */
    async updatePrimaryKeys(tableOrName: Table|string, columns: TableColumn[]): Promise<void> {
        throw new Error(`NYI: spanner: updatePrimaryKeys`);
        /*
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const clonedTable = table.clone();
        const columnNames = columns.map(column => column.name);
        const upQueries: string[] = [];
        const downQueries: string[] = [];

        // if table have generated column, we must drop AUTO_INCREMENT before changing primary constraints.
        const generatedColumn = clonedTable.columns.find(column => column.isGenerated && column.generationStrategy === "increment");
        if (generatedColumn) {
            const nonGeneratedColumn = generatedColumn.clone();
            nonGeneratedColumn.isGenerated = false;
            nonGeneratedColumn.generationStrategy = undefined;

            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${generatedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(generatedColumn, true)}`);
        }

        // if table already have primary columns, we must drop them.
        const primaryColumns = clonedTable.primaryColumns;
        if (primaryColumns.length > 0) {
            const columnNames = primaryColumns.map(column => `\`${column.name}\``).join(", ");
            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNames})`);
        }

        // update columns in table.
        clonedTable.columns
            .filter(column => columnNames.indexOf(column.name) !== -1)
            .forEach(column => column.isPrimary = true);

        const columnNamesString = columnNames.map(columnName => `\`${columnName}\``).join(", ");
        upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNamesString})`);
        downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`);

        // if we already have generated column or column is changed to generated, and we dropped AUTO_INCREMENT property before, we must bring it back
        const newOrExistGeneratedColumn = generatedColumn ? generatedColumn : columns.find(column => column.isGenerated && column.generationStrategy === "increment");
        if (newOrExistGeneratedColumn) {
            const nonGeneratedColumn = newOrExistGeneratedColumn.clone();
            nonGeneratedColumn.isGenerated = false;
            nonGeneratedColumn.generationStrategy = undefined;

            upQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${nonGeneratedColumn.name}\` ${this.buildCreateColumnSql(newOrExistGeneratedColumn, true)}`);
            downQueries.push(`ALTER TABLE ${this.escapeTableName(table)} CHANGE \`${newOrExistGeneratedColumn.name}\` ${this.buildCreateColumnSql(nonGeneratedColumn, true)}`);

            // if column changed to generated, we must update it in table
            const changedGeneratedColumn = clonedTable.columns.find(column => column.name === newOrExistGeneratedColumn.name);
            changedGeneratedColumn!.isGenerated = true;
            changedGeneratedColumn!.generationStrategy = "increment";
        }

        await this.executeQueries(upQueries, downQueries);
        this.replaceCachedTable(table, clonedTable);
        */
    }

    /**
     * Drops a primary key.
     */
    async dropPrimaryKey(tableOrName: Table|string): Promise<void> {
        throw new Error(`NYI: spanner: dropPrimaryKey`);
        /*
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const up = this.dropPrimaryKeySql(table);
        const down = this.createPrimaryKeySql(table, table.primaryColumns.map(column => column.name));
        await this.executeQueries(up, down);
        table.primaryColumns.forEach(column => {
            column.isPrimary = false;
        });
        */
    }

    /**
     * Creates a new unique constraint.
     */
    async createUniqueConstraint(tableOrName: Table|string, uniqueConstraint: TableUnique): Promise<void> {
        throw new Error(`NYI: spanner: createUniqueConstraint`);
    }

    /**
     * Creates a new unique constraints.
     */
    async createUniqueConstraints(tableOrName: Table|string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new Error(`NYI: spanner: createUniqueConstraints`);
    }

    /**
     * Drops an unique constraint.
     */
    async dropUniqueConstraint(tableOrName: Table|string, uniqueOrName: TableUnique|string): Promise<void> {
        throw new Error(`NYI: spanner: dropUniqueConstraint`);
    }

    /**
     * Drops an unique constraints.
     */
    async dropUniqueConstraints(tableOrName: Table|string, uniqueConstraints: TableUnique[]): Promise<void> {
        throw new Error(`NYI: spanner: dropUniqueConstraints`);
    }

    /**
     * Creates a new check constraint.
     */
    async createCheckConstraint(tableOrName: Table|string, checkConstraint: TableCheck): Promise<void> {
        throw new Error(`NYI: spanner: createCheckConstraint`);
    }

    /**
     * Creates a new check constraints.
     */
    async createCheckConstraints(tableOrName: Table|string, checkConstraints: TableCheck[]): Promise<void> {
        throw new Error(`NYI: spanner: createCheckConstraints`);
    }

    /**
     * Drops check constraint.
     */
    async dropCheckConstraint(tableOrName: Table|string, checkOrName: TableCheck|string): Promise<void> {
        throw new Error(`NYI: spanner: dropCheckConstraint`);
    }

    /**
     * Drops check constraints.
     */
    async dropCheckConstraints(tableOrName: Table|string, checkConstraints: TableCheck[]): Promise<void> {
        throw new Error(`NYI: spanner: dropCheckConstraints`);
    }

    /**
     * Creates a new foreign key. in spanner, it creates corresponding index too
     */
    async createForeignKey(tableOrName: Table|string, foreignKey: TableForeignKey): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new FK may be passed without name. In this case we generate FK name manually.
        if (!foreignKey.name)
            foreignKey.name = this.connection.namingStrategy.foreignKeyName(table.name, foreignKey.columnNames);

        const up = this.createForeignKeySql(table, foreignKey);
        const down = this.dropForeignKeySql(table, foreignKey);
        await this.executeQueries(up, down);
        table.addForeignKey(foreignKey);
    }

    /**
     * Creates a new foreign keys.
     */
    async createForeignKeys(tableOrName: Table|string, foreignKeys: TableForeignKey[]): Promise<void> {
        // in spanner, we achieve foreign key analogue with interleaved table.
        // const promises = foreignKeys.map(foreignKey => this.createForeignKey(tableOrName, foreignKey));
        // await Promise.all(promises);
    }

    /**
     * Drops a foreign key.
     */
    async dropForeignKey(tableOrName: Table|string, foreignKeyOrName: TableForeignKey|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const foreignKey = foreignKeyOrName instanceof TableForeignKey ? foreignKeyOrName : table.foreignKeys.find(fk => fk.name === foreignKeyOrName);
        if (!foreignKey)
            throw new Error(`Supplied foreign key was not found in table ${table.name}`);

        const up = this.dropForeignKeySql(table, foreignKey);
        const down = this.createForeignKeySql(table, foreignKey);
        await this.executeQueries(up, down);
        table.removeForeignKey(foreignKey);
    }

    /**
     * Drops a foreign keys from the table.
     */
    async dropForeignKeys(tableOrName: Table|string, foreignKeys: TableForeignKey[]): Promise<void> {
        const promises = foreignKeys.map(foreignKey => this.dropForeignKey(tableOrName, foreignKey));
        await Promise.all(promises);
    }

    /**
     * Creates a new index.
     */
    async createIndex(tableOrName: Table|string, index: TableIndex): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);

        // new index may be passed without name. In this case we generate index name manually.
        if (!index.name)
            index.name = this.connection.namingStrategy.indexName(table.name, index.columnNames, index.where);

        const up = this.createIndexSql(table, index);
        const down = this.dropIndexSql(table, index);
        await this.executeQueries(up, down);
        table.addIndex(index, true);
    }

    /**
     * Creates a new indices
     */
    async createIndices(tableOrName: Table|string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.createIndex(tableOrName, index));
        await Promise.all(promises);
    }

    /**
     * Drops an index.
     */
    async dropIndex(tableOrName: Table|string, indexOrName: TableIndex|string): Promise<void> {
        const table = tableOrName instanceof Table ? tableOrName : await this.getCachedTable(tableOrName);
        const index = indexOrName instanceof TableIndex ? indexOrName : table.indices.find(i => i.name === indexOrName);
        if (!index)
            throw new Error(`Supplied index was not found in table ${table.name}`);

        const up = this.dropIndexSql(table, index);
        const down = this.createIndexSql(table, index);
        await this.executeQueries(up, down);
        table.removeIndex(index, true);
    }

    /**
     * Drops an indices from the table.
     */
    async dropIndices(tableOrName: Table|string, indices: TableIndex[]): Promise<void> {
        const promises = indices.map(index => this.dropIndex(tableOrName, index));
        await Promise.all(promises);
    }

    /**
     * Clears all table contents.
     * Note: this operation uses SQL's TRUNCATE query which cannot be reverted in transactions.
     */
    async clearTable(tableOrName: Table|string): Promise<void> {
        if (tableOrName instanceof Table) {
            tableOrName = tableOrName.name;
        }
        const qb = this.connection.manager
            .createQueryBuilder(this)
            .delete()
            .from(tableOrName);
        return await this.delete(qb);
    }

    /**
     * Removes all tables from the currently connected database.
     * Be careful using this method and avoid using it in production or migrations
     * (because it can clear all your database).
     */
    async clearDatabase(database?: string): Promise<void> {
        const tables = await this.driver.getAllTablesForDrop(true);
        const keys = Object.keys(tables);
        const CONCURRENT_DELETION = 10;
        for (let i = 0; i < Math.ceil(keys.length / CONCURRENT_DELETION); i++) {
            const start = i * CONCURRENT_DELETION;
            const end = (i + 1) * CONCURRENT_DELETION;
            const range = keys.slice(start, end);
            if (range.length <= 0) {
                break;
            }
            await Promise.all(range.map(async (k) => {
                return this.dropTable(k);
            }));
        }
        /*const dbName = database ? database : this.driver.database;
        if (dbName) {
            const isDatabaseExist = await this.hasDatabase(dbName);
            if (!isDatabaseExist)
                return Promise.resolve();
        } else {
            throw new Error(`Can not clear database. No database is specified`);
        }

        await this.startTransaction();
        try {
            const disableForeignKeysCheckQuery = `SET FOREIGN_KEY_CHECKS = 0;`;
            const dropTablesQuery = `SELECT concat('DROP TABLE IF EXISTS \`', table_schema, '\`.\`', table_name, '\`') AS \`query\` FROM \`INFORMATION_SCHEMA\`.\`TABLES\` WHERE \`TABLE_SCHEMA\` = '${dbName}'`;
            const enableForeignKeysCheckQuery = `SET FOREIGN_KEY_CHECKS = 1;`;

            await this.query(disableForeignKeysCheckQuery);
            const dropQueries: ObjectLiteral[] = await this.query(dropTablesQuery);
            await Promise.all(dropQueries.map(query => this.query(query["query"])));
            await this.query(enableForeignKeysCheckQuery);

            await this.commitTransaction();

        } catch (error) {
            try { // we throw original error even if rollback thrown an error
                await this.rollbackTransaction();
            } catch (rollbackError) { }
            throw error;
        }*/
    }

    /**
     * create `schemas` table which describe additional column information such as
     * generated column's increment strategy or default value
     * @database: spanner's database object.
     */
    async createAndLoadSchemaTable(tableName: string): Promise<SpannerExtendSchemas> {
        const tableExist = await this.hasTable(tableName); // todo: table name should be configurable
        if (!tableExist) {
            await this.createTable(new Table(
                {
                    name: tableName,
                    columns: [
                        {
                            name: "table",
                            type: this.connection.driver.normalizeType({type: this.connection.driver.mappedDataTypes.migrationName}),
                            isPrimary: true,
                            isNullable: false
                        },
                        {
                            name: "column",
                            type: this.connection.driver.normalizeType({type: this.connection.driver.mappedDataTypes.migrationName}),
                            isPrimary: true,
                            isNullable: false
                        },
                        {
                            name: "type",
                            type: this.connection.driver.normalizeType({type: this.connection.driver.mappedDataTypes.migrationName}),
                            isPrimary: true,
                            isNullable: false
                        },
                        {
                            name: "value",
                            type: this.connection.driver.normalizeType({type: this.connection.driver.mappedDataTypes.migrationName}),
                            isNullable: false
                        },
                    ]
                },
            ));
        }

        const rawObjects: ObjectLiteral[] = await this.loadExtendSchemaTable(tableName);

        const schemas: SpannerExtendSchemas = {};
        for (const rawObject of rawObjects) {
            const table = rawObject["table"];
            if (!schemas[table]) {
                schemas[table] = {};
            }
            const tableSchemas = schemas[table];
            const column = rawObject["column"];
            if (!tableSchemas[column]) {
                tableSchemas[column] = {};
            }
            // value is stored in data as JSON.stringify form
            Object.assign(tableSchemas[column], this.createExtendSchemaObject(
                table, rawObject["type"], rawObject["value"]));
        }

        return schemas;
    }


    /**
     * Synchronizes table extend schema.
     * systemTables means internally used table, such as migrations.
     */
    public async syncExtendSchemas(metadata: EntityMetadata[]): Promise<SpannerExtendSchemas> {
        // specify true, to update `tables` to latest schema definition automatically
        const allSchemaObjects: { [k:string]:ObjectLiteral[] } = {};
        const raw = await this.loadExtendSchemaTable(
            this.driver.getSchemaTableName()
        );
        const systemTables = await this.driver.getSystemTables();
        raw.forEach((o) => {
            const t = o["table"];
            if (!allSchemaObjects[t]) {
                allSchemaObjects[t] = [];
            }
            allSchemaObjects[t].push(o);
        });
        const tableProps = (<SpannerExtendedTableProps[]>metadata)
        .concat(systemTables.map((st) => {
            return {
                name: st.name,
                columns: st.columns.map((c) => {
                    return new SpannerExtendedColumnPropsFromTableColumn(c);
                })
            }
        }));
        const oldNormalTables = Object.keys(allSchemaObjects);
        const newExtendSchemas: SpannerExtendSchemas = {};
        await Promise.all(tableProps.map(async (t) => {
            const oldTableIndex = oldNormalTables.indexOf(t.name);
            if (oldTableIndex >= 0) {
                oldNormalTables.splice(oldTableIndex, 1);
            }
            const promises: Promise<void[]>[] = [];
            const schemaObjectsByTable = allSchemaObjects[t.name] || [];
            const oldColumns = schemaObjectsByTable.map(o => o["column"]);
            for (const c of t.columns) {
                const oldColumnIndex = oldColumns.indexOf(c.databaseName);
                if (oldColumnIndex >= 0) {
                    oldColumns.splice(oldColumnIndex, 1);
                }
                //add, remove is not json stringified.
                const { add, remove } = this.getSyncExtendSchemaObjects(t, c);
                const addFiltered = add.filter((e) => {
                    // filter element which already added and not changed
                    return !schemaObjectsByTable.find(
                        (o) => o["column"] === e.column &&
                            o["type"] === e.type &&
                            o["value"] === e.value
                    );
                });
                const removeFiltered = remove.filter((e) => {
                    // filter element which does not exist
                    return schemaObjectsByTable.find(
                        (o) => o["column"] === e.column && o["type"] === e.type
                    );
                });
                if ((addFiltered.length + removeFiltered.length) > 0) {
                    promises.push(Promise.all([
                        ...addFiltered.map((e) => this.upsertExtendSchema(e.table, e.column, e.type, e.value)),
                        ...removeFiltered.map((e) => this.deleteExtendSchema(e.table, e.column, e.type))
                    ]));
                }
                if (add.length > 0) {
                    if (!newExtendSchemas[t.name]) {
                        newExtendSchemas[t.name] = {}
                    }
                    for (const a of add) {
                        newExtendSchemas[t.name][c.databaseName] = this.createExtendSchemaObject(
                            a.table, a.type, a.value
                        );
                    }
                }
            }
            // if column is no more exists in new entity metadata, remove all extend schema for such columns
            if (oldColumns.length > 0) {
                console.log('oldColumns', oldColumns);
                promises.push(Promise.all(oldColumns.map(async c => {
                    await this.deleteExtendSchema(t.name, c);
                })));
            }
            if (promises.length > 0) {
                await Promise.all(promises);
            }
        }));
        if (oldNormalTables.length > 0) {
            //console.log('oldNormalTables', oldNormalTables);
            await Promise.all(oldNormalTables.map(async (tableName) => {
                await this.deleteExtendSchema(tableName);
            }));
        }
        return newExtendSchemas;
    }

    /**
     * Creates a new exclusion constraint.
     */
    async createExclusionConstraint(table: Table|string, exclusionConstraint: TableExclusion): Promise<void> {
        throw new Error(`MySql does not support exclusion constraints.`);
    }

    /**
     * Creates new exclusion constraints.
     */
    async createExclusionConstraints(table: Table|string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new Error(`MySql does not support exclusion constraints.`);
    }

    /**
     * Drops a exclusion constraint.
     */
    async dropExclusionConstraint(table: Table|string, exclusionOrName: TableExclusion|string): Promise<void> {
        throw new Error(`MySql does not support exclusion constraints.`);
    }

    /**
     * Drops exclusion constraints.
     */
    async dropExclusionConstraints(table: Table|string, exclusionConstraints: TableExclusion[]): Promise<void> {
        throw new Error(`MySql does not support exclusion constraints.`);
    }



    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * helper for createAndLoadSchemaTable.
     * create schema object from schemas table column
     * @param type
     * @param value
     */
    protected createExtendSchemaObject(
        table: string, type: string, value: string
    ): SpannerExtendColumnSchema {
        const columnSchema: SpannerExtendColumnSchema = {};
        if (type === "generator") {
            if (value == "uuid") {
                columnSchema.generatorStorategy = "uuid";
                columnSchema.generator = RandomGenerator.uuid4;
            } else if (value == "increment") {
                columnSchema.generatorStorategy = "increment";
                // we automatically process increment generation storategy as uuid.
                // because spanner strongly discourage auto increment column.
                // TODO: if there is request, implement auto increment somehow.
                if (table !== "migrations") {
                    this.driver.connection.logger.log("warn", "column value generatorStorategy `increment` treated as `uuid` on spanner, due to performance reason.");
                }
                columnSchema.generator = SpannerDriver.randomInt64;
            }

        } else if (type === "default") {
            columnSchema.default = value;
            columnSchema.generator = this.driver.decodeDefaultValueGenerator(value);

        }

        return columnSchema;
    }

    protected verifyAndFillAutoGeneratedValues(
        table: Table,
        valuesSet?: ObjectLiteral|ObjectLiteral[]
    ): undefined|ObjectLiteral|ObjectLiteral[] {
        if (!valuesSet) {
            return valuesSet;
        }
        if (!(valuesSet instanceof Array)) {
            valuesSet = [valuesSet];
        }
        for (const values of valuesSet) {
            for (const column of table.columns) {
                if (values[column.name] === undefined) {
                    const value = this.driver.autoGenerateValue(table.name, column.name);
                    if (value !== undefined) {
                        values[column.name] = value;
                    }
                } else {
                    values[column.name] = this.driver.normalizeValue(
                        values[column.name], column.type);
                }
            }
        }
        return valuesSet;
    }

    protected verifyValues(
        table: Table,
        valuesSet?: ObjectLiteral|ObjectLiteral[]
    ): undefined|ObjectLiteral|ObjectLiteral[] {
        if (!valuesSet) {
            return valuesSet;
        }
        if (!(valuesSet instanceof Array)) {
            valuesSet = [valuesSet];
        }
        for (const values of valuesSet) {
            for (const column of table.columns) {
                if (values[column.name] !== undefined) {
                    values[column.name] = this.driver.normalizeValue(
                        values[column.name], column.type);
                }
            }
        }
        return valuesSet;
    }

    /**
     * helper for createAndLoadSchemaTable.
     * load formatted object from schema table
     */
    protected async loadExtendSchemaTable(tableName: string): Promise<ObjectLiteral[]> {
        return await this.connection.manager
            .createQueryBuilder(this)
            .select()
            .from(tableName, "")
            .getRawMany();
    }

    /**
     * get query string to examine select/update/upsert/delete keys.
     * null means value contains all key elements already.
     */
    protected async examineKeys<Entity>(table: Table, qb: QueryBuilder<Entity>, keysOnly?: boolean): Promise<ObjectLiteral[]|any[]|null> {
        /*
            qbが
            primary keyを含んでいる場合
            primary keyのカラムが１つ
            - qb.expressionMap.parameters.qb_ids or qb.whereExpressionのIN(...)の中身（パースの必要あり)
            primary keyのカラムが２つ以上
            - qb.expressionMap.nativeParametersの["id_" + index1 + "_" + index2]みたいなところに格納されている

            primary keyを完全には含まない場合
            entityを使わないupdate/deleteクエリ. とりあえずエラーを吐くが、将来的にはqb.whereExpressionとqb.parametersを使って
            下記のようなクエリを作りidを取得する.
        */
        const expressionMap = qb.expressionMap;
        let m: RegExpMatchArray|null;
        // check fast path
        if (expressionMap.parameters.qb_ids) {
            // non numeric single primary key
            const pc = table.primaryColumns[0];
            const keys = keysOnly ?
                expressionMap.parameters.qb_ids :
                (expressionMap.parameters.qb_ids as any[]).map(e => {
                    return {
                        [pc.name]: e
                    };
                });
                this.driver.connection.logger.log("info", `single primary key ${JSON.stringify(keys)}`);
            return keys;
        } else if (table.primaryColumns.length > 1) {
            const keys: any[] = [];
            for (const k of Object.keys(expressionMap.nativeParameters)) {
                m = k.match(/id_([0-9]+)_([0-9]+)/);
                if (m) {
                    const idx1 = Number(m[1]);
                    if (!keys[idx1]) {
                        keys[idx1] = keysOnly ? [] : <ObjectLiteral>{};
                    }
                    const idx2 = Number(m[2]);
                    if (keysOnly) {
                        keys[idx1][idx2] = expressionMap.nativeParameters[k];
                    } else {
                        const pc = table.primaryColumns[idx2];
                        keys[idx1][pc.name] = expressionMap.nativeParameters[k];
                    }
                }
            }
            if (keys.length > 0) {
                this.driver.connection.logger.log("info", `multiple primary keys ${JSON.stringify(keys)}`);
                return keys;
            }
        }
        const [query, parameters] = qb.getQueryAndParameters();
        const [params, types] = parameters;
        if (m = query.match(/IN\(([^)]+)\)/)) {
            const pc = table.primaryColumns[0];
            // parse IN statement (Number)
            // TODO: descriminator column uses IN statement. what is descriminator column?
            const parsed = m[1].split(",");
            const keys = keysOnly ? parsed : parsed.map(e => {
                return {
                    [pc.name]: Number(e.trim())
                };
            });
            this.driver.connection.logger.log("info", `single numeric primary ${JSON.stringify(keys)}`);
            return keys;
        }
        // not fast path. examine keys using where expression
        const idx = query.indexOf("WHERE");
        const sql = (
            `SELECT ${table.primaryColumns.map((c) => c.name).join(',')} FROM ${qb.escapedMainTableName}` +
            (idx >= 0 ? query.substring(idx) : "")
        );
        // run is both promisified
        const [results, err] = await (this.tx || this.databaseConnection).run({ sql, params, types, json:true });
        if (err) {
            this.driver.connection.logger.logQueryError(err, sql, [], this);
            throw err;
        }
        if (!results || results.length <= 0) {
            return [];
        }
        const keys = keysOnly ?
            (table.primaryColumns.length > 1 ?
                (results as ObjectLiteral[]).map(r => table.primaryColumns.map(pc => r[pc.name])) :
                (results as ObjectLiteral[]).map(r => r[table.primaryColumns[0].name])
            ) :
            results;
        this.driver.connection.logger.log("info", `queried keys ${JSON.stringify(keys)} by ${query} ${!!this.tx}`);
        return keys;
    }

    /**
     * wrapper to integrate request by transaction and table
     * connect() should be already called before this function invoked.
     */
    protected request(
        table: Table,
        method: "insert"|"update"|"upsert"|"deleteRows",
        ...args: any[]
    ): Promise<any> {
        if (this.driver.connection.options.logging) {
            this.driver.connection.logger.logQuery(
                `${method} ${table.name} ${this.isTransactionActive ? "tx" : "non-tx"}`, args[0]
            );
        }
        if (this.tx) {
            return this.tx[method](table.name, ...args);
        } else {
            return this.databaseConnection.table(table.name)[method](...args);
        }
    }

    /**
     * Handle select query
     */
    protected select<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        const [query, parameters] = qb.getQueryAndParameters();
        return this.query(query, parameters);
    }

    /**
     * Handle insert query
     */
    protected insert<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        return new Promise(async (ok, fail) => {
            try {
                const table = await this.getTable(qb.mainTableName).catch(fail);
                if (!table) {
                    fail(new Error(`insert: fatal: no such table ${qb.mainTableName}`));
                    return;
                }
                const vss = this.verifyAndFillAutoGeneratedValues(
                    table, qb.expressionMap.valuesSet);
                // NOTE: when transaction mode, callback (next args of vss) never called.
                // at transaction mode, this call just change queuedMutations_ property of this.tx,
                // and callback ignored.
                // then actual mutation will be done when commitTransaction is called.
                await this.request(table, 'insert', vss);
                ok(vss);
            } catch (e) {
                fail(e);
            }
        });
    }

    /**
     * Handle update query
     */
    protected update<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        return new Promise(async (ok, fail) => {
            try {
                const table = await this.getTable(qb.mainTableName).catch(fail);
                if (!table) {
                    fail(new Error(`update: fatal: no such table ${qb.mainTableName}`));
                    return;
                }
                const vss = this.verifyValues(table, qb.expressionMap.valuesSet);
                if (!vss || !(vss instanceof Array)) {
                    fail(new Error('only single value set can be used spanner update'));
                    return;
                }
                const value = <ObjectLiteral>vss[0]; //above vs checks assure this cast is valid
                const rows = await this.examineKeys(table, qb).catch(fail);
                if (rows === null) {
                    ok();
                    return;
                }
                for (const row of rows) {
                    Object.assign(row, value);
                }
                // callback not provided see comment of insert
                await this.request(table, 'update', rows);
                ok(rows);
            } catch (e) {
                fail(e);
            }
        });
    }

    /**
     * Handle upsert query
     */
    protected upsert<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        return new Promise(async (ok, fail) => {
            try {
                const table = await this.getTable(qb.mainTableName).catch(fail);
                if (!table) {
                    fail(new Error(`upsert: fatal: no such table ${qb.mainTableName}`));
                    return;
                }
                // for upsert, we assume all primary keys are provided, like insert.
                const vss = this.verifyAndFillAutoGeneratedValues(
                    table, qb.expressionMap.valuesSet);
                // callback not provided see comment of insert
                await this.request(table, 'upsert', vss);
                ok(vss);
            } catch (e) {
                fail(e);
            }
        });
    }

    /**
     * Handle delete query
     */
    protected delete<Entity>(qb: QueryBuilder<Entity>): Promise<any> {
        return new Promise(async (ok, fail) => {
            try {
                const table = await this.getTable(qb.mainTableName).catch(fail);
                if (!table) {
                    fail(new Error(`fatal: no such table ${qb.mainTableName}`));
                    return;
                }
                const rows = await this.examineKeys(table, qb, true).catch(fail);
                if (rows === null || (<any[]>rows).length <= 0) {
                    ok();
                    return;
                }
                // callback not provided see comment of insert
                await this.request(table, 'deleteRows', rows);
                ok();
            } catch (e) {
                fail(e);
            }
        });
    }
    /**
     * unescape table/database name
     */
    protected unescapeName(name: string): string {
        return name.replace(/`([^`]+)`/, "$1");
    }

    /**
     * convert parsed non-spanner sql to spanner ddl string and extend schema.
     * ast is generated by NearleyParser( = require('nearley').Parser)
     */
    protected toSpannerQueryAndSchema(ddl: string): [string, SpannerExtendSchemaSources, string] {
        this.driver.ddlParser.resetParser();
        const parser = this.driver.ddlParser.parser;
        parser.feed(ddl);

        const extendSchemas: SpannerExtendSchemaSources = {};
        const t = new SpannerDDLTransformer(this.driver.encodeDefaultValueGenerator.bind(this.driver));
        return [t.transform(parser.results[0], extendSchemas), extendSchemas, t.scopedTable];
    }

    /**
     * Handle administrative sqls as spanner API call
     */
    protected handleAdministrativeQuery(type: string, m: RegExpMatchArray): Promise<any>{
        return this.connect().then(async conn => {
            if (type == "CREATE") {
                const p = m[2].split(/\s/);
                if (p[0] == "DATABASE") {
                    let name = this.unescapeName(p[1]);
                    if (p[1] == "IF") {
                        if (p[2] != "NOT") {
                            return Promise.reject(new Error(`invalid query ${m[0]}`));
                        } else {
                            name = p[4];
                        }
                    }
                    return this.driver.createDatabase(name);
                }
            } else if (type == "DROP") {
                const p = m[2].split(/\s/);
                if (p[0] == "DATABASE") {
                    let name = this.unescapeName(p[1]);
                    if (p[1] == "IF") {
                        if (p[2] != "EXISTS") {
                            return Promise.reject(new Error(`invalid query ${m[0]}`));
                        } else {
                            name = p[3];
                        }
                    }
                    return this.driver.dropDatabase(name);
                } else if (p[0] == "TABLE") {
                    let name = this.unescapeName(p[1]);
                    return this.driver.dropTable(name);
                }
            }
            //others all updateSchema
            console.log('handleAdminQuery', m[0]);
            let sqls = m[0];
            if (!this.disableDDLParser && this.driver.ddlParser) {
                const ddl = m[0][m[0].length - 1] === ';' ? m[0] : (m[0] + ";");
                let extendSchames: SpannerExtendSchemaSources = {};
                [sqls, extendSchames] = this.toSpannerQueryAndSchema(ddl);
                console.log('handleAdminQuery', sqls, extendSchames);
                for (const tableName in extendSchames) {
                    const table = extendSchames[tableName];
                    for (const columnName in table) {
                        const column = table[columnName];
                        this.upsertExtendSchema(tableName, columnName, column.type, column.value);
                    }
                }
            }
            await Promise.all(
                sqls.split(';').
                filter(sql => !!sql).
                map(sql => conn.updateSchema(sql).then((data: any[]) => {
                    return data[0].promise();
                }))
            );
        });
    }

    /**
     * Loads all tables (with given names) from the database and creates a Table from them.
     */
    protected async loadTables(tableNames: string[]): Promise<Table[]> {
        // if no tables given then no need to proceed
        if (!tableNames || !tableNames.length)
            return [];

        return this.connect().then(async () => {
            const tables = await this.driver.loadTables(tableNames);
            return tables;
        });
    }

    /**
     * Builds create table sql
     */
    protected createTableSql(table: Table): string {
        const columnDefinitions = table.columns.map(column => this.buildCreateColumnSql(column, true)).join(", ");
        let sql = `CREATE TABLE ${this.escapeTableName(table)} (${columnDefinitions}`;

        // we create unique indexes instead of unique constraints, because MySql does not have unique constraints.
        // if we mark column as Unique, it means that we create UNIQUE INDEX.
        table.columns
            .filter(column => column.isUnique)
            .forEach(column => {
                const isUniqueIndexExist = table.indices.some(index => {
                    return index.columnNames.length === 1 && !!index.isUnique && index.columnNames.indexOf(column.name) !== -1;
                });
                const isUniqueConstraintExist = table.uniques.some(unique => {
                    return unique.columnNames.length === 1 && unique.columnNames.indexOf(column.name) !== -1;
                });
                if (!isUniqueIndexExist && !isUniqueConstraintExist)
                    table.indices.push(new TableIndex({
                        name: this.connection.namingStrategy.uniqueConstraintName(table.name, [column.name]),
                        columnNames: [column.name],
                        isUnique: true
                    }));
            });

        sql += `)`;

        if (table.primaryColumns.length > 0) {
            const columnNames = table.primaryColumns.map(column => `\`${column.name}\``).join(", ");
            sql += ` PRIMARY KEY (${columnNames})`;
        }

        if (table.foreignKeys.length > 0) {
            const foreignKeysSql = table.foreignKeys.map(fk => {
                let constraint = `INTERLEAVE IN PARENT ${this.escapeTableName(fk.referencedTableName)}`;
                if (fk.onDelete)
                    constraint += ` ON DELETE ${fk.onDelete}`;
                if (fk.onUpdate)
                    throw new Error(`NYI: spanner: fk.onUpdate`); //constraint += ` ON UPDATE ${fk.onUpdate}`;

                return constraint;
            }).join(", ");

            sql += `, ${foreignKeysSql}`;
        }

        //console.log('createTableSql', sql);

        return sql;
    }

    /**
     * Builds drop table sql
     */
    protected dropTableSql(tableOrName: Table|string): string {
        return `DROP TABLE ${this.escapeTableName(tableOrName)}`;
    }

    protected async dropTableSqlRecursive(tableOrName: Table|string, upQueries: string[], downQueries: string[]): Promise<void> {
        if (typeof tableOrName == 'string') {
            const table = await this.getTable(tableOrName);
            if (!table) {
                return; // already deleted
            }
            tableOrName = table;
        }
        await Promise.all(tableOrName.foreignKeys.map((fk) => {
            this.dropTableSqlRecursive(fk.referencedTableName, upQueries, downQueries);
        }))
        upQueries.push(this.dropTableSql(tableOrName));
        downQueries.push(this.createTableSql(tableOrName));
    }

    /**
     * Builds create index sql.
     */
    protected createIndexSql(table: Table, index: TableIndex): string {
        //TODO: somehow supports interleave and storing clause
        const columns = index.columnNames.map(columnName => `\`${columnName}\``).join(", ");
        let indexType = "";
        if (index.isUnique)
            indexType += "UNIQUE ";
        if (index.isSpatial)
            indexType += "NULL_FILTERED ";
        if (index.isFulltext)
            throw new Error(`NYI: spanner: index.isFulltext`); //indexType += "FULLTEXT ";
        return `CREATE ${indexType}INDEX \`${index.name}\` ON ${this.escapeTableName(table)}(${columns})`;
    }

    /**
     * Builds drop index sql.
     */
    protected dropIndexSql(table: Table, indexOrName: TableIndex|string): string {
        //throw new Error('should not drop any index');
        let indexName = indexOrName instanceof TableIndex ? indexOrName.name : indexOrName;
        return `DROP INDEX \`${indexName}\``;
    }

    /**
     * Builds create primary key sql.
     */
    protected createPrimaryKeySql(table: Table, columnNames: string[]): string {
        const columnNamesString = columnNames.map(columnName => `\`${columnName}\``).join(", ");
        return `ALTER TABLE ${this.escapeTableName(table)} ADD PRIMARY KEY (${columnNamesString})`;
    }

    /**
     * Builds drop primary key sql.
     */
    protected dropPrimaryKeySql(table: Table): string {
        return `ALTER TABLE ${this.escapeTableName(table)} DROP PRIMARY KEY`;
    }

    /**
     * Builds create foreign key sql.
     */
    protected createForeignKeySql(table: Table, foreignKey: TableForeignKey): string {
        throw new Error('NYI: spanner: column level foreign key declaration');
        /* const referencedColumnNames = foreignKey.referencedColumnNames.map(column => `\`${column}\``).join(",");
        const columnNames = foreignKey.columnNames.map(column => `\`${column}\``).join(",");
        const fkName = foreignKey.name || `${referencedColumnNames}By${foreignKey.columnNames.join()}`;
        let sql = `CREATE INDEX ${fkName} ON
            ${this.escapeTableName(table.name)}\(${referencedColumnNames}, ${columnNames}\)
            INTERLEAVE IN ${this.escapeTableName(foreignKey.referencedTableName)}`;
        if (foreignKey.onDelete)
            sql += ` ON DELETE ${foreignKey.onDelete}`;
        if (foreignKey.onUpdate)
            throw new Error(`NYI: spanner: foreignKey.onUpdate`); //sql += ` ON UPDATE ${foreignKey.onUpdate}`;

        return sql; */
    }

    /**
     * Builds drop foreign key sql.
     */
    protected dropForeignKeySql(table: Table, foreignKeyOrName: TableForeignKey|string): string {
        throw new Error('NYI: spanner: column level foreign key declaration');
        /* const foreignKeyName = foreignKeyOrName instanceof TableForeignKey ? foreignKeyOrName.name : foreignKeyOrName;
        return `DROP INDEX \`${foreignKeyName}\` ON ${this.escapeTableName(table)}`; */
    }

    protected parseTableName(target: Table|string) {
        const tableName = target instanceof Table ? target.name : target;
        return {
            database: tableName.indexOf(".") !== -1 ? tableName.split(".")[0] : this.driver.database,
            tableName: tableName.indexOf(".") !== -1 ? tableName.split(".")[1] : tableName
        };
    }

    /**
     * Escapes given table name.
     */
    protected escapeTableName(target: Table|string, disableEscape?: boolean): string {
        const tableName = target instanceof Table ? target.name : target;
        let splits = tableName.split(".");
        if (splits.length > 1) {
            //omit database name to avoid spanner table name parse error.
            splits = splits.slice(1);
        }
        return splits.map(i => disableEscape ? i : `\`${i}\``).join(".");
    }

    /**
     * Builds a part of query to create/change a column.
     */
    protected buildCreateColumnSql(column: TableColumn, skipPrimary: boolean, skipName: boolean = false) {
        let c = "";
        if (skipName) {
            c = this.connection.driver.createFullType(column);
        } else {
            c = `\`${column.name}\` ${this.connection.driver.createFullType(column)}`;
        }
        if (column.asExpression)
            throw new Error(`NYI: spanner: column.asExpression`); // c += ` AS (${column.asExpression}) ${column.generatedType ? column.generatedType : "VIRTUAL"}`;

        // if you specify ZEROFILL for a numeric column, MySQL automatically adds the UNSIGNED attribute to that column.
        if (column.zerofill) {
            throw new Error(`NYI: spanner: column.zerofill`); // c += " ZEROFILL";
        } else if (column.unsigned) {
            throw new Error(`NYI: spanner: column.unsigned`); // c += " UNSIGNED";
        }

        // spanner
        if (column.enum)
            throw new Error(`NYI: spanner: column.enum`); // c += ` (${column.enum.map(value => "'" + value + "'").join(", ")})`;

        // spanner only supports utf8
        if (column.charset && column.charset.toLowerCase().indexOf("utf8") >= 0)
            throw new Error(`NYI: spanner: column.charset = ${column.charset}`); // c += ` CHARACTER SET "${column.charset}"`;
        if (column.collation)
            throw new Error(`NYI: spanner: column.collation`); // c += ` COLLATE "${column.collation}"`;

        if (!column.isNullable)
            c += " NOT NULL";

        // explicit nullable modifier not supported. silently ignored.
        // if (column.isNullable) c += " NULL";

        // primary key can be specified only at table creation
        // not error but does not take effect here.
        // if (column.isPrimary && !skipPrimary) c += " PRIMARY KEY";

        // spanner does not support any generated columns, nor default value.
        // we should create metadata table and get information about generated columns
        // if (column.isGenerated && column.generationStrategy === "increment") {
        // }

        // does not support comment.
        if (column.comment)
            throw new Error(`NYI: spanner: column.comment`); //c += ` COMMENT '${column.comment}'`;

        // spanner ddl does not support any default value except SpannerColumnUpdateWithCommitTimestamp
        // other default value is supported by extend schema table
        if (column.default !== undefined && column.default !== null) {
            if (column.default === SpannerColumnUpdateWithCommitTimestamp) {
                c += `OPTIONS (allow_commit_timestamp=true)`
            }
        }

        // does not support on update
        if (column.onUpdate)
            throw new Error(`NYI: spanner: column.onUpdate`); //c += ` ON UPDATE ${column.onUpdate}`;

        return c;
    }

    protected buildCreateColumnOptionsSql(column: TableColumn): string {
        return "";
    }

    protected replaceCachedTable(table: Table, changedTable: Table|null): void {
        if (changedTable) {
            super.replaceCachedTable(table, changedTable);
            this.driver.setTable(changedTable);
        } else {
            const index = this.loadedTables.findIndex((t) => t.name == table.name);
            if (index >= 0) {
                this.loadedTables.splice(index, 1);
            }
        }
    }

    protected getSyncExtendSchemaObjects(table: SpannerExtendedTableProps, column: SpannerExtendedColumnProps): {
        add: {table:string, column:string, type: string, value: string}[],
        remove: {table:string, column:string, type: string}[]
    } {
        const ret = {
            add: <{table:string, column:string, type: string, value: string}[]>[],
            remove: <{table:string, column:string, type: string}[]>[]
        };
        if (column.default !== undefined) {
            const defaultValue = this.driver.encodeDefaultValueGenerator(column.default);
            ret.add.push({table: table.name, column: column.databaseName, type: "default", value: defaultValue});
        } else {
            if (column.isNullable) {
                const defaultValue = this.driver.encodeDefaultValueGenerator(null);
                ret.add.push({table: table.name, column: column.databaseName, type: "default", value: defaultValue});
            } else {
                ret.remove.push({table: table.name, column: column.databaseName, type: "default"});
            }
        }
        if (column.generationStrategy) {
            ret.add.push({table: table.name, column: column.databaseName, type: "generator", value: column.generationStrategy});
        } else {
            ret.remove.push({table: table.name, column: column.databaseName, type: "generator"});
        }
        return ret;
    }

    protected async deleteExtendSchema(table: string, column?: string, type?: string): Promise<void> {
        const wh = column ? (
            type ?
                `\`table\` = '${table}' AND \`column\` = '${column}' AND \`type\` = '${type}'` :
                `\`table\` = '${table}' AND \`column\` = '${column}'`
        ) : (
            `\`table\` = '${table}'`
        );
        const qb = this.connection.manager
            .createQueryBuilder(this)
            .delete()
            .from(this.driver.options.schemaTableName || "schemas")
            .where(wh);
        return this.delete(qb);
    }

    protected async upsertExtendSchema(table: string, column: string, type: string, value: string): Promise<void> {
        const qb = this.connection.manager
            .createQueryBuilder(this)
            .update(this.driver.options.schemaTableName || "schemas")
            .set(<any>{table, column, type, value})
            .where(`\`table\` = '${table}' AND \`column\` = '${column}' AND \`type\` = '${type}'`);
        return this.upsert(qb);
    }
}
