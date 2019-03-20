import {Driver} from "../Driver";
import {ColumnType} from "../types/ColumnTypes";
import {SpannerConnectionOptions} from "./SpannerConnectionOptions";
import {RdbmsSchemaBuilder} from "../../schema-builder/RdbmsSchemaBuilder";
import {SpannerQueryRunner} from "./SpannerQueryRunner";
import {Connection} from "../../connection/Connection";
import {MappedColumnTypes} from "../types/MappedColumnTypes";
import {DataTypeDefaults} from "../types/DataTypeDefaults";
import {DriverPackageNotInstalledError} from "../../error/DriverPackageNotInstalledError";
import {PlatformTools} from "../../platform/PlatformTools";
import {ColumnMetadata} from "../../metadata/ColumnMetadata";
import {TableColumn} from "../../schema-builder/table/TableColumn";
import {TableOptions} from "../../schema-builder/options/TableOptions";
import {TableColumnOptions} from "../../schema-builder/options/TableColumnOptions";
import {TableIndexOptions} from "../../schema-builder/options/TableIndexOptions";
import {TableForeignKeyOptions} from "../../schema-builder/options/TableForeignKeyOptions";
import {TableUniqueOptions} from "../../schema-builder/options/TableUniqueOptions";
import {EntityMetadata} from "../../metadata/EntityMetadata";
import {DateUtils} from "../../util/DateUtils";
import {SpannerDatabase, SpannerExtendSchemas} from "./SpannerRawTypes";
import {Table} from "../../schema-builder/table/Table";
import {ObjectLiteral} from "../../common/ObjectLiteral";
import {ValueTransformer} from "../../decorator/options/ValueTransformer";
import {SpannerUtil} from "./SpannerUtil";
import * as Long from "long";
if (process.env.PRELOAD_SPANNER_DEPENDENCY) {
    require('@google-cloud/spanner');
    require('sql-ddl-to-json-schema');
}

//import { filter } from "minimatch";


export const SpannerColumnUpdateWithCommitTimestamp = "commit_timestamp";

/**
 * Organizes communication with MySQL DBMS.
 */
export class SpannerDriver implements Driver {

    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection used by driver.
     */
    connection: Connection;

    /**
     * Spanner underlying library.
     */
    spannerLib: any;
    spanner: {
        client: any;
        instance: any;
        database: SpannerDatabase;
    } | null;

    /**
     * because spanner's schema change cannot be done transactionally, 
     * we ignore start/commit/rollback Transaction during schema change phase
     */
    enableTransaction: boolean;

    /**
     * ddl parser to use mysql migrations as spanner ddl. 
     * https://github.com/duartealexf/sql-ddl-to-json-schema
     */
    ddlParser: any;


    // -------------------------------------------------------------------------
    // Public Implemented Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: SpannerConnectionOptions;

    /**
     * Master database used to perform all write queries.
     */
    database?: string;

    /**
     * Indicates if replication is enabled.
     */
    isReplicated: boolean = false;

    /**
     * Indicates if tree tables are supported by this driver.
     */
    treeSupport = true;

    /**
     * Gets list of supported column data types by a driver.
     *
     * @see https://www.tutorialspoint.com/mysql/mysql-data-types.htm
     * @see https://dev.mysql.com/doc/refman/5.7/en/data-types.html
     */
    supportedDataTypes: ColumnType[] = [
        "int64",
        "bytes",
        "bool",
        "date",
        "float64",
        "string",
        "timestamp",
    ];

    /**
     * Gets list of spatial column data types.
     */
    spatialTypes: ColumnType[] = [
    ];

    /**
     * Gets list of column data types that support length by a driver.
     */
    withLengthColumnTypes: ColumnType[] = [
        "bytes",
        "string",
    ];

    /**
     * Gets list of column data types that support length by a driver.
     */
    withWidthColumnTypes: ColumnType[] = [
        "bytes",
        "string",
    ];

    /**
     * Gets list of column data types that support precision by a driver.
     */
    withPrecisionColumnTypes: ColumnType[] = [
        "float64",
    ];

    /**
     * Gets list of column data types that supports scale by a driver.
     */
    withScaleColumnTypes: ColumnType[] = [
        "float64",
    ];

    /**
     * Gets list of column data types that supports UNSIGNED and ZEROFILL attributes.
     */
    unsignedAndZerofillTypes: ColumnType[] = [
    ];

    /**
     * ORM has special columns and we need to know what database column types should be for those columns.
     * Column types are driver dependant.
     */
    mappedDataTypes: MappedColumnTypes = {
        createDate: "timestamp",
        createDatePrecision: 20,
        createDateDefault: "CURRENT_TIMESTAMP(6)",
        updateDate: "timestamp",
        updateDatePrecision: 20,
        updateDateDefault: "CURRENT_TIMESTAMP(6)",
        version: "int64",
        treeLevel: "int64",
        migrationId: "string",
        migrationName: "string",
        migrationTimestamp: "timestamp",
        cacheId: "string",
        cacheIdentifier: "string",
        cacheTime: "int64",
        cacheDuration: "int64",
        cacheQuery: "string",
        cacheResult: "string",
    };

    /**
     * Default values of length, precision and scale depends on column data type.
     * Used in the cases when length/precision/scale is not specified by user.
     */
    dataTypeDefaults: DataTypeDefaults = {
        "string": { length: 255 },
        "bytes": { length: 255 },
    };

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection: Connection) {
        this.connection = connection;
        this.options = connection.options as SpannerConnectionOptions;
        this.enableTransaction = false;

        // load mysql package
        this.loadDependencies();

    }

    // -------------------------------------------------------------------------
    // static Public Methods (SpannerDriver specific)
    // -------------------------------------------------------------------------
    static updateTableWithExtendSchema(
        db: SpannerDatabase, 
        extendSchemas: SpannerExtendSchemas, 
        ignoreColumnNotFound: boolean
    ) {
        db.schemas = extendSchemas;
        for (const tableName in db.tables) {
            const table = db.tables[tableName];
            const extendSchema = extendSchemas[tableName];
            if (extendSchema) {
                for (const columnName in extendSchema) {
                    const columnSchema = extendSchema[columnName];
                    const column = table.findColumnByName(columnName);
                    if (column) {
                        column.isGenerated = !!columnSchema.generator;
                        column.default = columnSchema.default;
                        column.generationStrategy = columnSchema.generatorStorategy;
                    } else if (!ignoreColumnNotFound) {
                        throw new Error(`extendSchema for column ${columnName} exists but table does not have it`);
                    }
                }
            } else {
                // console.log('extendSchema for ', tableName, 'does not exists', extendSchemas);
            }
            // console.log('table', tableName, table);
        }
    }
    static randomInt64(): string {
        const bytes = SpannerUtil.randomBytes(8);
        const as_numbers: number[] = [];
        // TODO: is there any better(faster) way? 
        for (const b of bytes) {
            as_numbers.push(b);
        }
        return Long.fromBytes(as_numbers, true).toString();
    }

    // -------------------------------------------------------------------------
    // Public Methods (SpannerDriver specific)
    // -------------------------------------------------------------------------
    /**
     * returns spanner database object. used as databaseConnection of query runner.
     */
    async getDatabaseHandle(): Promise<any> {
        if (!this.spanner) {
            await this.connect();
            if (!this.spanner) {
                throw new Error('fail to reconnect');
            }
        }
        return this.spanner.database.handle;
    }
    async getAllTablesForDrop(force?:boolean): Promise<{[name:string]:Table}> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        this.spanner.database.tables = {}
        await this.loadTables(this.getSchemaTableName());
        return this.spanner.database.tables;
    }
    // get list of table names which has actual Table but not metadata. 
    // (eg. migrations)
    systemTableNames(): string[] {
        return [
            this.options.migrationsTableName || "migrations",
            "query-result-cache"
        ];
    }
    // get list of tables which has actual Table but not metadata. 
    // (eg. migrations)
    async getSystemTables(): Promise<Table[]> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        const db = this.spanner.database;
        await this.loadTables(this.getSchemaTableName());
        return this.systemTableNames()
            .map((name) => db.tables[name])
            .filter((t) => !!t);
    }
    getExtendSchemas(): SpannerExtendSchemas {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        return this.spanner.database.schemas || {};
    }
    /**
     * create and drop database of arbiter name. 
     * if name equals this.options.database, change driver state accordingly
     */
    createDatabase(name: string): Promise<any> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        if (name == this.options.database) {
            return Promise.resolve(this.spanner.database.handle);
        }
        return this.spanner.instance.database(name).get({autoCreate:true});
    }
    dropDatabase(name: string): Promise<void> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        if (name == this.options.database) {
            return this.spanner.database.handle.delete.then(() => {
                this.disconnect();
            });
        }
        return this.spanner.instance.database(name).delete();
    }
    /**
     * set tables object cache. 
     */
    setTable(table: Table) {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        this.spanner.database.tables[table.name] = table;
        // if system table is updated, setup extend schemas again.
        if (this.systemTableNames().indexOf(table.name) !== -1) {
            this.setupExtendSchemas(this.spanner.database, false);
        }
    }
    async dropTable(tableName: string): Promise<void> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        const t = this.spanner.database.tables[tableName];
        if (t) {
            console.log(`deleting table[${tableName}]...`);
            await this.spanner.database.handle.table(tableName)
            .delete()
            .then((data: any) => {
                // need to wait until table deletion
                return data[0].promise();
            })
            .then(() => {
                if (this.spanner) {
                    delete this.spanner.database.tables[tableName];
                    if (this.getSchemaTableName() == tableName) {
                        this.spanner.database.schemas = null;
                    }
                }        
            });
            console.log(`deleted table[${tableName}]`);
        } else {
            console.log(`deleting table[${tableName}]`, 'not exists', this.spanner.database.tables);
        }
    }
    /**
     * load tables. cache them into this.spanner.databases too.
     * @param tableNames table names which need to load. 
     */
    loadTables(tableNames: string[]|Table|string): Promise<Table[]> {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        if (typeof tableNames === 'string') {
            tableNames = [tableNames];
        } else if (tableNames instanceof Table) {
            tableNames = [tableNames.name];
        }
        const database = this.spanner.database;
        return (async () => {        
            const tables = await Promise.all(tableNames.map(async (tableName: string) => {
                let [dbname, name] = tableName.split(".");
                if (!name) {
                    name = dbname;
                }
                if (Object.keys(database.tables).length === 0) {
                    const handle = database.handle;
                    const schemas = await handle.getSchema();
                    database.tables = await this.parseSchema(schemas);
                }
                return database.tables[name];
            }));
            return tables.filter((t) => !!t);
        })();
    }
    getDatabases(): string[] {
        return Object.keys([this.options.database]);
    }
    isSchemaTable(table: Table): boolean {
        return this.getSchemaTableName() === table.name;
    }
    getSchemaTableName(): string {
        return this.options.schemaTableName || "schemas";
    }
    getTableEntityMetadata(): EntityMetadata[] {
        return this.connection.entityMetadatas.filter(metadata => metadata.synchronize && metadata.tableType !== "entity-child");
    }
    autoGenerateValue(tableName: string, columnName: string): any {
        if (!this.spanner) {
            throw new Error('connect() driver first');
        }
        const database = this.spanner.database;
        if (!database.schemas || 
            !database.schemas[tableName] || 
            !database.schemas[tableName][columnName]) {
            return undefined;
        }
        const generator = database.schemas[tableName][columnName].generator;
        if (!generator) {
            return undefined;
        }
        return generator();
    }
    encodeDefaultValueGenerator(value: any): string {
        const defaultValue = typeof(value) === 'function' ? value() : value;
        if (defaultValue === this.mappedDataTypes.createDateDefault) {
            return defaultValue;
        } else {
            return JSON.stringify(defaultValue);
        }
    }
    decodeDefaultValueGenerator(value: string): () => any {
        if (value === this.mappedDataTypes.createDateDefault) {
            return () => new Date();
        } else {
            const parsedDefault = JSON.parse(value);
            return () => parsedDefault;
        }
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     */
    async connect(): Promise<void> {
        if (!this.spanner) {
			const Spanner = this.spannerLib.Spanner;
            // create objects
            const client = new Spanner({
                projectId: this.options.projectId,
            });
            const instance = client.instance(this.options.instanceId);
            const database = instance.database(this.options.database);
            await database.get({autoCreate: true});
            this.spanner = {
                client, instance,
                database: {
                    handle: database,
                    tables: {},
                    schemas: null,
                }
            };
            //actual database creation done in createDatabase (called from SpannerQueryRunner)
            return Promise.resolve();
        }
    }

    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     * here update extend schema. 
     */
    afterConnect(): Promise<void> {
        return (async () => {
            if (!this.spanner) {
                throw new Error('connect() driver first');
            }
            // TODO: translate
            // synchronizeはspannerに可能なスキーマの変更だけを行い、
            // generationStorategyやdefaultなどは変更することができず、
            // schemasテーブルを更新する必要がある。
            // また、this.spanner.database.tablesはschemasテーブルの情報をマージした状態で
            // synchronizeに入る必要があるため、ここでsetupExtendSchemasする。
            // そうしない場合、synchronizeでそれ関連の属性で差分が出てしまい、
            // 結果的にalterでは変更できない属性(defaultやgenerationStorategy)を変更しようとして
            // エラーになるため。ただしdropSchemaする場合には直後に削除されてしまい無駄なのでafterBootStepで行う
            if (!this.options.dropSchema) {
                await this.setupExtendSchemas(this.spanner.database, false);
            }
        })();
    }

    /**
     * Makes any action after any synchronization happens (e.g. sync extend schema table in Spanner driver)
     */
    afterBootStep(event: "DROP_DATABASE"|"RUN_MIGRATION"|"SYNCHRONIZE"|"FINISH"): Promise<void> {
        return (async () => {
            if (!this.spanner) {
                throw new Error('connect() driver first');
            }
            switch (event) {
            case "DROP_DATABASE":
                await this.setupExtendSchemas(this.spanner.database, false);
                break;
            case "RUN_MIGRATION":
                break;
            case "SYNCHRONIZE":
                break;
            case "FINISH":
                await this.setupExtendSchemas(this.spanner.database, true);
                this.enableTransaction = true;
                break;
            }
        })();
    }

    /**
     * Closes connection with the database.
     */
    async disconnect(): Promise<void> {
        this.spanner = null;
    }

    /**
     * Creates a schema builder used to build and sync a schema.
     */
    createSchemaBuilder() {
        return new RdbmsSchemaBuilder(this.connection);
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode: "master"|"slave" = "master") {
        return new SpannerQueryRunner(this);
    }

    /**
     * Replaces parameters in the given sql with special escaping character
     * and an array of parameter names to be passed to a query.
     */
    escapeQueryWithParameters(sql: string, parameters: ObjectLiteral, nativeParameters: ObjectLiteral): [string, any[]] {
        // written values (for update) are likely to put in nativeParameter
        // OTOH read values (for select, update, delete) are likely to put in parameter. 
        if (!parameters || !Object.keys(parameters).length)
            return [sql, [nativeParameters]];

        const keys = Object.keys(parameters).map(parameter => "(:(\\.\\.\\.)?" + parameter + "\\b)").join("|");
        sql = sql.replace(new RegExp(keys, "g"), (key: string) => {
            let value: any;
            let paramName: string;
            let placeHolder: string;
            if (key.substr(0, 4) === ":...") {
                paramName = key.substr(4);
                placeHolder = `UNNEST(@${paramName})`;
            } else {
                paramName = key.substr(1);
                placeHolder = `@${paramName}`;
            }
            value = parameters[paramName];

            if (value instanceof Function) {
                return value();

            } else {
                return placeHolder;
            }
        // IN (UNNEST(@val)) causes error
        }).replace(/\s+IN\s+\(([^)]+)\)/g, (key: string, p1: string) => {
            return ` IN ${p1}`;
        }); // todo: make replace only in value statements, otherwise problems
        return [sql, [parameters]];
    }

    /**
     * Escapes a column name.
     */
    escape(columnName: string): string {
        return "`" + columnName + "`";
    }

    /**
     * Build full table name with database name, schema name and table name.
     * E.g. "myDB"."mySchema"."myTable"
     * but spanner does not allow to prefix database name, we just returns table name.
     */
    buildTableName(tableName: string, schema?: string, database?: string): string {
        return tableName;
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type and metadata.
     */
    preparePersistentValue(value: any, columnMetadata: ColumnMetadata): any {
        return this.normalizeValue(value, this.normalizeType({type:columnMetadata.type}), columnMetadata.transformer);
    }
    normalizeValue(value: any, type: any, transformer?: ValueTransformer): any {
        if (transformer)
            value = transformer.to(value);

        if (value === null || value === undefined)
            return value;

        if (type === "timestamp" || 
            type === "date" || 
            type === Date) {
            if (typeof(value) === 'number') {
                // convert millisecond numeric timestamp to date object. 
                // because @google/spanner does not accept it
                return new Date(value); 
            }
            return DateUtils.mixedDateToDate(value);

        } /*else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value);

        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value);
        } */ else if (
            type == Number ||
            type == String || 
            type == Boolean ||
            type == "int64" ||
            type == "float64" ||
            type == "bool" ||
            type == "string" ||
            type == "bytes") {
            return value;
        } else if (
            type == "uuid"
        ) {
            return value.toString();
        }

        throw new Error(`spanner driver does not support '${type}' column type`);
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        try {
            return this.preparePersistentValue(value, columnMetadata);
        } catch (e) {
            if (columnMetadata.transformer)
                return columnMetadata.transformer.from(value);
            throw e;
        }
    }

    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: { type: ColumnType, length?: number|string, precision?: number|null, scale?: number }): string {
        if (column.type === Number || column.type.toString().indexOf("int") !== -1) {
            return "int64";

        } else if (column.type.toString().indexOf("float") !== -1 ||
            column.type.toString().indexOf("double") !== -1 ||
            column.type.toString().indexOf("dec") !== -1) {
            return "float64";
        }
        else if (column.type === String || 
            column.type.toString().indexOf("char") !== -1 ||
            column.type.toString().indexOf("text") !== -1) {
            return "string";

        } else if (column.type === Date ||
            column.type.toString().indexOf("time") !== -1) {
            return "timestamp";

        } else if ((column.type as any) === Buffer || (column.type as any) === Uint8Array ||
            column.type.toString().indexOf("binary") !== -1 ||
            column.type.toString().indexOf("blob") !== -1 ||
            column.type.toString().indexOf("char") !== -1) {
            return "bytes";

        } else if (column.type === Boolean ||
            column.type === "bit") {
            return "bool";

        } else if (column.type === "simple-array" || column.type === "simple-json" || column.type === "uuid") {
            return "string";

        } else {
            return column.type as string || "";
        }
    }

    /**
     * Normalizes "default" value of the column.
     */
    normalizeDefault(columnMetadata: ColumnMetadata): string {
        const defaultValue = columnMetadata.default;

        if (columnMetadata.isUpdateDate) {
            return SpannerColumnUpdateWithCommitTimestamp;

        } else if (typeof defaultValue === "number") {
            return "" + defaultValue;

        } else if (typeof defaultValue === "boolean") {
            return defaultValue === true ? "true" : "false";

        } else if (typeof defaultValue === "function") {
            return defaultValue();

        } else if (typeof defaultValue === "string") {
            return `'${defaultValue}'`;

        } else {
            return defaultValue;
        }
    }

    /**
     * Normalizes "isUnique" value of the column.
     */
    normalizeIsUnique(column: ColumnMetadata): boolean {
        return column.entityMetadata.indices.some(idx => idx.isUnique && idx.columns.length === 1 && idx.columns[0] === column);
    }

    /**
     * Returns default column lengths, which is required on column creation.
     */
    getColumnLength(column: ColumnMetadata|TableColumn): string {
        if (column.length)
            return column.length.toString();

        switch (column.type) {
            case String:
            case "string":
                return "255";
            case "bytes":
                return "255";
            default:
                return "";
        }
    }

    /**
     * Creates column type definition including length, precision and scale
     */
    createFullType(column: TableColumn): string {
        let type = column.type;

        // used 'getColumnLength()' method, because MySQL requires column length for `varchar`, `nvarchar` and `varbinary` data types
        if (this.getColumnLength(column)) {
            type += `(${this.getColumnLength(column)})`;

        } else if ((<string[]>this.withWidthColumnTypes).indexOf(type) >= 0 && column.width) {
            type += `(${column.width})`;

        } else if ((<string[]>this.withPrecisionColumnTypes).indexOf(type) >= 0) {
            if (column.precision !== null && column.precision !== undefined && column.scale !== null && column.scale !== undefined) {
                type += `(${column.precision},${column.scale})`;

            } else if (column.precision !== null && column.precision !== undefined) {
                type += `(${column.precision})`;
            }
        }

        if (column.isArray)
            type = `Array<${type}>`;

        //console.log('createFullType', type, column);
        return type;
    }

    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection(): Promise<any> {
        if (!this.spanner) {
            throw new Error(`no active database`);
        }
        return Promise.resolve(this.spanner.database.handle);
    }

    /**
     * Obtains a new database connection to a slave server.
     * Used for replication.
     * If replication is not setup then returns master (default) connection's database connection.
     */
    obtainSlaveConnection(): Promise<any> {
        return this.obtainMasterConnection();
    }

    /**
     * Creates generated map of values generated or returned by database after INSERT query.
     */
    createGeneratedMap(metadata: EntityMetadata, insertResult: any): ObjectLiteral|undefined {
        return insertResult;
    }

    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[] {
        //console.log('columns', tableColumns);
        const filtered = columnMetadatas.filter(columnMetadata => {
            const tableColumn = tableColumns.find(c => c.name === columnMetadata.databaseName);
            if (!tableColumn)
                return false; // we don't need new columns, we only need exist and changed

            /* console.log("changed property ==========================================");
            console.log("table.column:", columnMetadata.entityMetadata.tableName, columnMetadata.databaseName);
            if (tableColumn.name !== columnMetadata.databaseName)
                console.log("name:", tableColumn.name, columnMetadata.databaseName);
            if (tableColumn.type.toLowerCase() !== this.normalizeType(columnMetadata).toLowerCase())
                console.log("type:", tableColumn.type.toLowerCase(), this.normalizeType(columnMetadata).toLowerCase());
            if (tableColumn.length !== columnMetadata.length)
               console.log("length:", tableColumn.length, columnMetadata.length.toString());
            if (tableColumn.width !== columnMetadata.width)
               console.log("width:", tableColumn.width, columnMetadata.width);
            // if (tableColumn.precision !== columnMetadata.precision)
               // console.log("precision:", tableColumn.precision, columnMetadata.precision);
            if (tableColumn.scale !== columnMetadata.scale)
               console.log("scale:", tableColumn.scale, columnMetadata.scale);
            if (tableColumn.zerofill !== columnMetadata.zerofill)
               console.log("zerofill:", tableColumn.zerofill, columnMetadata.zerofill);
            if (tableColumn.unsigned !== columnMetadata.unsigned)
               console.log("unsigned:", tableColumn.unsigned, columnMetadata.unsigned);
            if (tableColumn.asExpression !== columnMetadata.asExpression)
               console.log("asExpression:", tableColumn.asExpression, columnMetadata.asExpression);
            if (tableColumn.generatedType !== columnMetadata.generatedType)
               console.log("generatedType:", tableColumn.generatedType, columnMetadata.generatedType);
            // if (tableColumn.comment !== columnMetadata.comment)
               // console.log("comment:", tableColumn.comment, columnMetadata.comment);
            if (tableColumn.default !== columnMetadata.default)
               console.log("default:", tableColumn.default, columnMetadata.default);
            if (!this.compareDefaultValues(this.normalizeDefault(columnMetadata), tableColumn.default))
               console.log("default changed:", !this.compareDefaultValues(this.normalizeDefault(columnMetadata), tableColumn.default));
            if (tableColumn.onUpdate !== columnMetadata.onUpdate)
               console.log("onUpdate:", tableColumn.onUpdate, columnMetadata.onUpdate);
            if (tableColumn.isPrimary !== columnMetadata.isPrimary)
               console.log("isPrimary:", tableColumn.isPrimary, columnMetadata.isPrimary);
            if (tableColumn.isNullable !== columnMetadata.isNullable)
               console.log("isNullable:", tableColumn.isNullable, columnMetadata.isNullable);
            if (tableColumn.isUnique !== this.normalizeIsUnique(columnMetadata))
               console.log("isUnique:", tableColumn.isUnique, this.normalizeIsUnique(columnMetadata));
            if (tableColumn.isGenerated !== columnMetadata.isGenerated)
               console.log("isGenerated:", tableColumn.isGenerated, columnMetadata.isGenerated);
            console.log("=========================================="); */

            return tableColumn.name !== columnMetadata.databaseName
                || tableColumn.type.toLowerCase() !== this.normalizeType(columnMetadata).toLowerCase() 
                || tableColumn.length !== columnMetadata.length
                || tableColumn.width !== columnMetadata.width
                // || tableColumn.precision !== columnMetadata.precision : spanner has no precision specifier
                || tableColumn.scale !== columnMetadata.scale
                || tableColumn.zerofill !== columnMetadata.zerofill
                || tableColumn.unsigned !== columnMetadata.unsigned
                || tableColumn.asExpression !== columnMetadata.asExpression
                || tableColumn.generatedType !== columnMetadata.generatedType
                // || tableColumn.comment !== columnMetadata.comment // todo
                || !this.compareDefaultValues(this.normalizeDefault(columnMetadata), tableColumn.default)
                || tableColumn.onUpdate !== columnMetadata.onUpdate
                || tableColumn.isPrimary !== columnMetadata.isPrimary
                || tableColumn.isNullable !== columnMetadata.isNullable
                || tableColumn.isUnique !== this.normalizeIsUnique(columnMetadata)
                || (columnMetadata.generationStrategy !== "uuid" && tableColumn.isGenerated !== columnMetadata.isGenerated);
        });

        //console.log('filtered', filtered);
        return filtered;
    }

    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     * for Spanner, no auto assigned value (default/generatedStorategy(uuid, increment)) at database side. 
     * every such values are defined in client memory, so just return insertValue. 
     */
    isReturningSqlSupported(): boolean {
        return true;
    }

    /**
     * Returns true if driver supports uuid values generation on its own.
     */
    isUUIDGenerationSupported(): boolean {
        return false;
    }

    /**
     * Creates an escaped parameter.
     */
    createParameter(parameterName: string, index: number): string {
        return `@${parameterName}`;
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Loads all driver dependencies.
     */
    protected loadDependencies(): void {
        try {
            this.spannerLib = PlatformTools.load('@google-cloud/spanner');  // try to load first supported package
            if (this.options.migrationDDLType) {
                const parser = PlatformTools.load('sql-ddl-to-json-schema');
                this.ddlParser = new parser(this.options.migrationDDLType);
            } else {
                this.ddlParser = undefined;
            }
            /*
             * Some frameworks (such as Jest) may mess up Node's require cache and provide garbage for the 'mysql' module
             * if it was not installed. We check that the object we got actually contains something otherwise we treat
             * it as if the `require` call failed.
             *
             * @see https://github.com/typeorm/typeorm/issues/1373
             */
            [this.spannerLib, this.ddlParser].map((lib) => {
                if (lib && Object.keys(lib).length === 0) {
                    throw new Error("dependency was found but it is empty.");
                }
            });

        } catch (e) {
            console.log(e);
            throw new DriverPackageNotInstalledError("Spanner", "@google-cloud/spanner");
        }
    }

    /**
     * Checks if "DEFAULT" values in the column metadata and in the database are equal.
     */
    protected compareDefaultValues(columnMetadataValue: string, databaseValue: string): boolean {
        if (typeof columnMetadataValue === "string" && typeof databaseValue === "string") {
            // we need to cut out "'" because in mysql we can understand returned value is a string or a function
            // as result compare cannot understand if default is really changed or not
            columnMetadataValue = columnMetadataValue.replace(/^'+|'+$/g, "");
            databaseValue = databaseValue.replace(/^'+|'+$/g, "");
        }

        return columnMetadataValue === databaseValue;
    }

    /**
     * parse typename and return additional information required by TableColumn object.
     */
    protected parseTypeName(typeName: string): {
        typeName: string;
        isArray: boolean;
        length?: string;
    } {
        typeName = typeName.toLowerCase();
        const tm = typeName.match(/([^\(]+)\((\d+)\)/);
        if (tm) {
            const typeDefault = this.dataTypeDefaults[tm[1]];
            return {
                typeName: tm[1],
                isArray: false,
                length: typeDefault && typeDefault.length && 
                    tm[2] == typeDefault.length.toString() ? undefined : tm[2]
            };
        } 
        const am = typeName.match(/([^<]+)<(\w+)>/);
        if (am) {
            return {
                typeName,
                isArray: true,
            }
        }
        return {
            typeName,
            isArray: false,
        }
    }

     /**
     * parse output of database.getSchema to generate Table object
     */
    protected async parseSchema(schemas: any): Promise<{[tableName: string]: Table}> {
        this.connection.logger.log("info", schemas);
        const tableOptionsMap: {[tableName: string]: TableOptions} = {};
        for (const stmt of schemas[0]) {
            // console.log('stmt', stmt);
            // stmt =~ /CREATE ${tableName} (IF NOT EXISTS) (${columns}) ${interleaves}/
            /* example. 
            CREATE TABLE migrations (
                id STRING(255) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                name STRING(255) NOT NULL,
            ) PRIMARY KEY(id)

            CREATE INDEX IDX_908fdaa14b12b506f5c2371001 ON Item(ownerId, item_id)

            in below regex, ,(?=\s*\)) is matched `,\n)` just before PRIMARY KEY
            */

           // variable for storing parse results
           const indices: TableIndexOptions[] = [];
           const foreignKeys: TableForeignKeyOptions[] = [];
           const uniques: TableUniqueOptions[] = [];
           const columns: TableColumnOptions[] = [];

           const m = stmt.match(/\s*CREATE\s+TABLE\s+(\w+)\s?[^\(]*\(([\s\S]*?),(?=\s*\))\s*\)([\s\S]*)/);
            if (!m) {
                // idxStmt =~ CREATE (UNIQUE|NULL_FILTERED) INDEX ${name} ON ${tableName}(${columns}) (INTERLEAVE IN ${parentTableName})
                const im = stmt.match(/(\w[\w\s]+?)\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)(.*)/);
                if (im) {
                    //console.log('process as index', im);
                    if (im[5] && im[5].indexOf("INTERLEAVE") >= 0) {
                        // interleaved index. this seems to be same as `partially interleaved table`.
                        // we use interleaved table for relation, difficult to use this feature
                        // because no way to specify interleaved table from @Index annotation.
                        // one hack is use where property of IndexOptions to specify table name.
                        throw new Error('TODO: spanner: interleaved index support');
                    } else {
                        // normal index
                        const tableIndexOptions = {
                            name: im[2],
                            columnNames: im[4].split(",").map((e: string) => e.trim()),
                            isUnique: im[1].indexOf("UNIQUE") >= 0,
                            isSpatial: im[1].indexOf("NULL_FILTERED") >= 0,
                            isFulltext: false
                        };
                        const tableOptions = tableOptionsMap[im[3]];
                        if (tableOptions) {
                            tableOptions.indices = tableOptions.indices || [];
                            tableOptions.indices.push(tableIndexOptions);
                            if (tableIndexOptions.isUnique) {
                                tableOptions.uniques = tableOptions.uniques || [];
                                tableOptions.columns = tableOptions.columns || [];
                                tableOptions.uniques.push({
                                    name: tableIndexOptions.name,
                                    columnNames: tableIndexOptions.columnNames
                                });
                                for (const uniqueColumnName of tableIndexOptions.columnNames) {
                                    const options = tableOptions.columns.find(c => c.name == uniqueColumnName);
                                    if (options) {
                                        options.isUnique = true;
                                    } else {
                                        throw new Error(`unique columns should exists in table ${im[3]} <= ${uniqueColumnName}`);
                                    }
                                }
                            }
                        } else {
                            throw new Error(`index ddl appears before main table ddl: ${im[3]}`);
                        }
                    }
                    continue;
                } else {
                    throw new Error("invalid ddl format:" + stmt);
                }
            }
            const tableName: string = m[1]; 
            const columnStmts: string = m[2];
            const indexStmts: string = m[3];
            // parse columns
            for (const columnStmt of columnStmts.split(',')) {
                // console.log('columnStmt', `[${columnStmt}]`);
                const cm = columnStmt.match(/(\w+)\s+([\w\(\)]+)\s*([^\n]*)/);
                if (!cm) {
                    throw new Error("invalid ddl column format:" + columnStmt);
                }
                const type = this.parseTypeName(cm[2]);
                // check and store constraint with m[3]
                columns.push({
                    name: cm[1],
                    type: type.typeName,
                    isNullable: cm[3].indexOf("NOT NULL") < 0,
                    isGenerated: false, // set in updateTableWithExtendSchema
                    isPrimary: false, // set afterwards
                    isUnique: false, // set afterwards
                    isArray: type.isArray,
                    length: type.length ? type.length : undefined, 
                    default: undefined, // set in updateTableWithExtendSchema
                    generationStrategy: undefined, // set in updateTableWithExtendSchema
                });
            }
            // parse primary and interleave statements
            // probably tweak required (need to see actual index/interleave statements format)
            if (indexStmts == null) {
                continue;
            }
            for (const idxStmt of (indexStmts.match(/(\w+[\w\s]+\([^)]+\)[^,]*)/g) || [])) {
                // console.log('idxStmt', idxStmt);
                // distinguish index and foreignKey. fk should contains INTERLEAVE
                if (idxStmt.indexOf("INTERLEAVE") == 0) {
                    // foreighkey
                    // idxStmt =~ INTERLEAVE IN PARENT ${this.escapeTableName(fk.referencedTableName)}
                    const im = idxStmt.match(/INTERLEAVE\s+IN\s+PARENT\s+(\w+)/);
                    if (im) {
                        foreignKeys.push({
                            name: tableName,
                            columnNames: [`${m[2]}_id`],
                            referencedTableName: im[1],
                            referencedColumnNames: [] // set afterwards (primary key column of referencedTable)
                        });
                    } else {
                        throw new Error("invalid ddl interleave format:" + idxStmt);
                    }
                } else if (idxStmt.indexOf("PRIMARY") == 0) {
                    // primary key
                    // idxStmt =~ PRIMARY KEY (${columns})
                    const pm = idxStmt.match(/PRIMARY\s+KEY\s*\(([^)]+)\)/);
                    if (pm) {
                        for (const primaryColumnName of pm[1].split(',').map(e => e.trim())) {
                            const options = columns.find(c => c.name == primaryColumnName);
                            if (options) {
                                options.isPrimary = true;
                            }
                        }
                    } else {
                        throw new Error("invalid ddl pkey format:" + idxStmt);
                    }
                }
            }
            tableOptionsMap[tableName] = {
                name: tableName,
                columns,
                indices,
                foreignKeys,
                uniques
            };
        }
        const result: { [tableName:string]: Table } = {};
        for (const tableName in tableOptionsMap) {
            // console.log('tableOptions', tableName, tableOptionsMap[tableName]);
            result[tableName] = new Table(tableOptionsMap[tableName]);
        }
        return result;
    }

    protected async setupExtendSchemas(db: SpannerDatabase, afterSync: boolean) {
        const maybeSchemaChange = this.options.dropSchema || this.options.synchronize || this.options.migrationsRun
        const queryRunner = this.createQueryRunner("master");
        // path1: recover previous extend schema stored in database
        const extendSchemas = await queryRunner.createAndLoadSchemaTable(
            this.getSchemaTableName()
        );
        const ignoreColumnNotFound = !afterSync;
        if (maybeSchemaChange && afterSync) {
            const database = this.spanner!.database;
            const handle = database.handle;
            const schemas = await handle.getSchema();
            database.tables = await this.parseSchema(schemas);
        }
        SpannerDriver.updateTableWithExtendSchema(db, extendSchemas, ignoreColumnNotFound);

        // path2: fill the difference from schema which is defined in code, if schema may change
        if (maybeSchemaChange) {
            const newExtendSchemas = await queryRunner.syncExtendSchemas(
                this.getTableEntityMetadata()
            )
            SpannerDriver.updateTableWithExtendSchema(db, newExtendSchemas, ignoreColumnNotFound);
        }
    }
}
