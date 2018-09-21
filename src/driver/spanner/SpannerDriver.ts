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
import {SpannerDatabase} from "./SpannerRawTypes";
import {Table} from "../../schema-builder/table/Table";
import {ObjectLiteral} from "../../common/ObjectLiteral";
import {DataTypeNotSupportedError} from "../../error/DataTypeNotSupportedError";

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
        active?: string;
        databases: { [key:string]:SpannerDatabase };
    };


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
        migrationId: "int64",
        migrationName: "string",
        migrationTimestamp: "timestamp",
        cacheId: "int64",
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
        "timestamp": { width: 20 },
        "date": { width: 10 },
        "bool": { width: 1 },
        "bytes": { length: 255 },
        "float64": { precision: 22 },
        "int64": { width: 20 }
    };

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection: Connection) {
        this.connection = connection;
        this.options = connection.options as SpannerConnectionOptions;

        // load mysql package
        this.loadDependencies();

    }

    // -------------------------------------------------------------------------
    // Public Methods (SpannerDriver specific)
    // -------------------------------------------------------------------------
    createDatabase(name?: string): Promise<any> {
        if (!name) {
            if (this.spanner.active) {
                name = this.spanner.active;
            } else {
                throw new Error(`no active database`);
            }
        }
        if (!this.spanner.databases[name]) {
            return (async () => {
                const database = this.spanner.instance.database(name);
                await database.get({autoCreate: true});
                this.spanner.databases[name] = {
                    handle: database,
                    tables: {},
                }
                if (!this.spanner.active) {
                    this.spanner.active = name;
                }
                return database;
            })();
        }
        return Promise.resolve(this.spanner.databases[name].handle);
    }
    dropDatabase(name: string): Promise<any> {
        if (!this.spanner.databases[name]) {
            //TODO: just ignoring error maybe better.
            return Promise.reject(new Error(`database: ${name} does not exists`));
        }
        return (async() => {
            await this.spanner.databases[name].handle.delete();
            delete this.spanner.databases[name];
            if (this.spanner.active == name) {
                const ks = Object.keys(this.spanner.databases);
                if (ks.length > 0) {
                    this.spanner.active = ks[0];
                } else {
                    this.spanner.active = "";
                }
            }
        })();
    }
    loadTables(tableNames: string[]|Table|string): Promise<Table[]> {
        if (typeof tableNames === 'string') {
            tableNames = [tableNames];
        } else if (tableNames instanceof Table) {
            tableNames = [tableNames.name];
        }
        const schemasMap: { 
            [dbname: string]: {
                [tableName: string]: Table
            } 
        } = {};
        return Promise.all(tableNames.map(async (tableName:string) => {
            let [dbname, name] = tableName.split(".");
            if (!name) {
                if (!this.spanner.active) {
                    throw new Error(`table name does not contain database name and no active database exists`);
                }
                name = dbname;
                dbname = this.spanner.active;
            }
            if (!schemasMap[dbname]) {
                const database = await this.createDatabase(dbname);
                const schemas = await database.getSchema();
                schemasMap[dbname] = this.parseSchema(schemas);
            }
            return schemasMap[dbname][name];
        }));
    }
    getDatabases(): string[] {
        return Object.keys(this.spanner.databases);
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Performs connection to the database.
     */
    async connect(): Promise<void> {
        if (!this.spanner || !this.spanner.client) {
			const Spanner = this.spannerLib.Spanner;
            // create objects
            const client = new Spanner({
                projectId: this.options.projectId,
            });
            const instance = client.instance(this.options.instanceId);
            this.spanner = {
                client, instance,
                active: this.options.database,
                databases: {}
            };
            return (async () => {
                await this.createDatabase(this.options.database);
            })();
        }
    }

    /**
     * Makes any action after connection (e.g. create extensions in Postgres driver).
     */
    afterConnect(): Promise<void> {
        return Promise.resolve();
    }

    /**
     * Closes connection with the database.
     */
    async disconnect(): Promise<void> {
        this.spanner = {
            client: false,
            instance: false,
            active: undefined,
            databases: {},
        };
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
        const escapedParameters: any[] = Object.keys(nativeParameters).map(key => nativeParameters[key]);
        if (!parameters || !Object.keys(parameters).length)
            return [sql, escapedParameters];

        const keys = Object.keys(parameters).map(parameter => "(:(\\.\\.\\.)?" + parameter + "\\b)").join("|");
        sql = sql.replace(new RegExp(keys, "g"), (key: string) => {
            let value: any;
            if (key.substr(0, 4) === ":...") {
                value = parameters[key.substr(4)];
            } else {
                value = parameters[key.substr(1)];
            }

            if (value instanceof Function) {
                return value();

            } else {
                escapedParameters.push(value);
                return "?";
            }
        }); // todo: make replace only in value statements, otherwise problems
        return [sql, escapedParameters];
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
        if (columnMetadata.transformer)
            value = columnMetadata.transformer.to(value);

        if (value === null || value === undefined)
            return value;

        if (columnMetadata.type === "timestamp" || 
            columnMetadata.type === "date" || 
            columnMetadata.type === Date) {
            return DateUtils.mixedDateToDate(value);

        } /*else if (columnMetadata.type === "simple-array") {
            return DateUtils.simpleArrayToString(value);

        } else if (columnMetadata.type === "simple-json") {
            return DateUtils.simpleJsonToString(value);
        } */ else if (
            columnMetadata.type == "int64" ||
            columnMetadata.type == "float64" ||
            columnMetadata.type == "bool" ||
            columnMetadata.type == "string" ||
            columnMetadata.type == "bytes") {
            return value;
        }

        throw new DataTypeNotSupportedError(columnMetadata, columnMetadata.type, "spanner");
    }

    /**
     * Prepares given value to a value to be persisted, based on its column type or metadata.
     */
    prepareHydratedValue(value: any, columnMetadata: ColumnMetadata): any {
        if (value === null || value === undefined)
            return value;

        if (columnMetadata.type === "timestamp" || 
            columnMetadata.type === "date" || 
            columnMetadata.type === Date) {
            value = DateUtils.mixedDateToDate(value);

        } /*else if (columnMetadata.type === "simple-array") {
            value = DateUtils.simpleArrayToString(value);

        } else if (columnMetadata.type === "simple-json") {
            value = DateUtils.simpleJsonToString(value);
        } */ else if (
            columnMetadata.type == "int64" ||
            columnMetadata.type == "float64" ||
            columnMetadata.type == "bool" ||
            columnMetadata.type == "string" ||
            columnMetadata.type == "bytes") {
        } else {
            throw new DataTypeNotSupportedError(columnMetadata, columnMetadata.type, "spanner");
        }

        if (columnMetadata.transformer)
            value = columnMetadata.transformer.from(value);

        return value;
    }

    /**
     * Creates a database type from a given column metadata.
     */
    normalizeType(column: { type: ColumnType, length?: number|string, precision?: number|null, scale?: number }): string {
        if (column.type === Number || column.type === "integer") {
            return "int64";

        } else if (column.type === String || column.type === "nvarchar") {
            return "string";

        } else if (column.type === Date) {
            return "timestamp";

        } else if ((column.type as any) === Buffer) {
            return "bytes";

        } else if (column.type === Boolean) {
            return "bool";

        } else if (column.type === "simple-array" || column.type === "simple-json") {
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

        if (typeof defaultValue === "number") {
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

        } else if (column.width) {
            type += `(${column.width})`;

        } else if (column.precision !== null && column.precision !== undefined && column.scale !== null && column.scale !== undefined) {
            type += `(${column.precision},${column.scale})`;

        } else if (column.precision !== null && column.precision !== undefined) {
            type += `(${column.precision})`;
        }

        if (column.isArray)
            type = `Array<${type}>`;

        return type;
    }

    /**
     * Obtains a new database connection to a master server.
     * Used for replication.
     * If replication is not setup then returns default connection's database connection.
     */
    obtainMasterConnection(): Promise<any> {
        if (!this.spanner.active) {
            throw new Error(`no active database`);
        }
        return Promise.resolve(this.spanner.databases[this.spanner.active].handle);
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
        if (insertResult) {
            throw new Error(`NYI: spanner: createGeneratedMap`);
        }
        return undefined;
    }

    /**
     * Differentiate columns of this table and columns from the given column metadatas columns
     * and returns only changed.
     */
    findChangedColumns(tableColumns: TableColumn[], columnMetadatas: ColumnMetadata[]): ColumnMetadata[] {
        return columnMetadatas.filter(columnMetadata => {
            const tableColumn = tableColumns.find(c => c.name === columnMetadata.databaseName);
            if (!tableColumn)
                return false; // we don't need new columns, we only need exist and changed

            // console.log("table:", columnMetadata.entityMetadata.tableName);
            // console.log("name:", tableColumn.name, columnMetadata.databaseName);
            // console.log("type:", tableColumn.type, this.normalizeType(columnMetadata));
            // console.log("length:", tableColumn.length, columnMetadata.length);
            // console.log("width:", tableColumn.width, columnMetadata.width);
            // console.log("precision:", tableColumn.precision, columnMetadata.precision);
            // console.log("scale:", tableColumn.scale, columnMetadata.scale);
            // console.log("zerofill:", tableColumn.zerofill, columnMetadata.zerofill);
            // console.log("unsigned:", tableColumn.unsigned, columnMetadata.unsigned);
            // console.log("asExpression:", tableColumn.asExpression, columnMetadata.asExpression);
            // console.log("generatedType:", tableColumn.generatedType, columnMetadata.generatedType);
            // console.log("comment:", tableColumn.comment, columnMetadata.comment);
            // console.log("default:", tableColumn.default, columnMetadata.default);
            // console.log("default changed:", !this.compareDefaultValues(this.normalizeDefault(columnMetadata), tableColumn.default));
            // console.log("onUpdate:", tableColumn.onUpdate, columnMetadata.onUpdate);
            // console.log("isPrimary:", tableColumn.isPrimary, columnMetadata.isPrimary);
            // console.log("isNullable:", tableColumn.isNullable, columnMetadata.isNullable);
            // console.log("isUnique:", tableColumn.isUnique, this.normalizeIsUnique(columnMetadata));
            // console.log("isGenerated:", tableColumn.isGenerated, columnMetadata.isGenerated);
            // console.log("==========================================");

            return tableColumn.name !== columnMetadata.databaseName
                || tableColumn.type !== this.normalizeType(columnMetadata)
                || tableColumn.length !== columnMetadata.length
                || tableColumn.width !== columnMetadata.width
                || tableColumn.precision !== columnMetadata.precision
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
    }

    /**
     * Returns true if driver supports RETURNING / OUTPUT statement.
     */
    isReturningSqlSupported(): boolean {
        return false;
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
            /*
             * Some frameworks (such as Jest) may mess up Node's require cache and provide garbage for the 'mysql' module
             * if it was not installed. We check that the object we got actually contains something otherwise we treat
             * it as if the `require` call failed.
             *
             * @see https://github.com/typeorm/typeorm/issues/1373
             */
            if (Object.keys(this.spannerLib).length === 0) {
                throw new Error("'spanner' was found but it is empty.");
            }

        } catch (e) {
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
     * 
     */
    protected linkOptions(optionsMap: {[tableName: string]: TableOptions} ) {

    }

    /**
     * 
     */
    protected parseTypeName(typeName: string): {
        typeName: string;
        isArray: boolean;
        length: number;
    } {
        const tm = typeName.match(/([^\(]+)\((\d+)\)/);
        if (tm) {
            return {
                typeName: tm[1],
                isArray: false,
                length: Number(tm[2])
            };
        } 
        const am = typeName.match(/([^<]+)<(\w+)>/);
        if (am) {
            return {
                typeName,
                isArray: true,
                length: 1
            }
        }
        return {
            typeName,
            isArray: false,
            length: 1
        }
    }

     /**
     * parse output of database.getSchema to generate Table object
     */
    protected parseSchema(schemas: any): {[tableName: string]: Table} {
        const tableOptionsMap: {[tableName: string]: TableOptions} = {};
        for (const stmt of schemas[0]) {
            // console.log('stmt', stmt);
            // stmt =~ /CREATE ${tableName} (IF NOT EXISTS) (${columns}) ${interleaves}/
            /* example. 
            CREATE TABLE migrations (
                id INT64 NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                name STRING(255) NOT NULL,
            ) PRIMARY KEY(id)
            in below regex, ,(?=\s*\)) is matched `,\n)` just before PRIMARY KEY
            */
            const m = stmt.match(/\s*CREATE\s+TABLE\s+(\w+)\s?[^\(]*\(([\s\S]*?),(?=\s*\))\s*\)([\s\S]*)/);
            if (!m) {
                throw new Error("invalid ddl format:" + stmt);
            }
            const tableName: string = m[1]; 
            const columnStmts: string = m[2];
            const indexStmts: string = m[3];
            // parse columns
            const columns: TableColumnOptions[] = [];
            for (const columnStmt of columnStmts.split(',')) {
                // console.log('columnStmt', `[${columnStmt}]`);
                const cm = columnStmt.match(/(\w+)\s+([\w\(\)]+)\s+([^\n]*)/);
                if (!cm) {
                    throw new Error("invalid ddl column format:" + columnStmt);
                }
                const type = this.parseTypeName(cm[2]);
                // check and store constraint with m[3]
                columns.push({
                    name: cm[1],
                    type: type.typeName,
                    isNullable: cm[3].indexOf("NOT NULL") >= 0,
                    isGenerated: false,
                    isPrimary: false, // set afterwards
                    isUnique: false, // set afterwards
                    isArray: type.isArray,
                    length: type.length.toString(), 
                    zerofill: true,
                    unsigned: false,
                });
            }
            // parse primary and interleave statements
            const indices: TableIndexOptions[] = [];
            const foreignKeys: TableForeignKeyOptions[] = [];
            const uniques: TableUniqueOptions[] = [];
            // probably tweak required (need to see actual index/interleave statements format)
            for (const idxStmt of indexStmts.split(',')) {
                // distinguish index and foreignKey. fk should contains INTERLEAVE
                if (idxStmt.indexOf("INTERLEAVE") >= 0) {
                    // foreighkey
                    // idxStmt =~ INTERLEAVE IN PARENT ${this.escapeTableName(fk.referencedTableName)}
                    idxStmt.replace(/INTERLEAVE\s+IN\s+PARENT\s+(\w+)\((\w+)\)/, (m) => {
                        foreignKeys.push({
                            name: tableName,
                            columnNames: [`${m[2]}_id`],
                            referencedTableName: m[2],
                            referencedColumnNames: [] // set afterwards (primary key column of referencedTable)
                        });
                        return m[0];
                    });
                } else if (idxStmt.indexOf("PRIMARY") >= 0) {
                    // primary key
                    // idxStmt =~ PRIMARY KEY (${columns})
                    idxStmt.replace(/PRIMARY\s+KEY\s+\((\w+)\)/g, (m) => {
                        for (const primaryColumnName of m[1].split(',').map(e => e.trim())) {
                            const options = columns.find(c => c.name == primaryColumnName);
                            if (options) {
                                options.isPrimary = true;
                            }
                        }
                        return m[0];
                    });
                } else {
                    // index
                    // idxStmt =~ (UNIQUE|NULL_FILTERED) INDEX ${name} ON ${tableName}(${columns})
                    idxStmt.replace(/(\w*)\s?INDEX\s+(\w+)\s+ON\s(\w+)\((\w+)\)(.*)/g, (m) => {
                        const tableIndexOptions = {
                            name: m[2],
                            columnNames: m[4].split(",").map(e => e.trim()),
                            isUnique: m[1].indexOf("UNIQUE") >= 0,
                            isSpatial: m[1].indexOf("NULL_FILTERED") >= 0
                        };
                        indices.push(tableIndexOptions);
                        if (tableIndexOptions.isUnique) {
                            uniques.push({
                                name: tableIndexOptions.name,
                                columnNames: tableIndexOptions.columnNames
                            });
                            for (const uniqueColumnName in tableIndexOptions.columnNames) {
                                const options = columns.find(c => c.name == uniqueColumnName);
                                if (options) {
                                    options.isUnique = true;
                                }
                            }
                        }
                        return m[0];
                    });
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
        this.linkOptions(tableOptionsMap);
        const result: { [tableName:string]: Table } = {};
        for (const tableName in tableOptionsMap) {
            result[tableName] = new Table(tableOptionsMap[tableName]);
            //console.log('table', tableName, result[tableName]);
        }
        return result;
    }
}
