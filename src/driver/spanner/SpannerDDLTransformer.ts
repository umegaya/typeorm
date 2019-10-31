import {
    SpannerExtendSchemaSources,
} from "./SpannerRawTypes";

type ASTTransformer = (ast: any, extendSchemas: SpannerExtendSchemaSources) => string;
type DataTypeChecker = (origType: string) => string;

interface ASTTransformerSet {
    [operation: string]: ASTTransformer;
}

interface Column {
    name: string;
    sort: string;
};
type IndexType = {
    unique: boolean;
    sparse: boolean;
} | string;
interface Index {
    name: string;
    type: IndexType;
    table: string;
    columns: Column[];
};

export class SpannerDDLTransformer {
    defaultValueEncoder: (value: any) => string;
    scopedTable: string;
    scopedColumn?: string;
    scopedColumnType?: string;
    scopedIndex?: string;
    primaryKeyColumns: Column[];
    indices: Index[];

    constructor(defaultValueEncoder: (value: any) => string) {
        this.defaultValueEncoder = defaultValueEncoder;
        this.primaryKeyColumns = [];
        this.indices = [];
    }
    transform(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        const set = <ASTTransformerSet><any>this;
        return (set[ast.id] || set["transform"]).call(this, ast.def, extendSchemas);
    }
    // common
    protected addExtendSchema(extendSchemas: SpannerExtendSchemaSources, type: string, value: string) {
        const key = this.scopedColumn || this.scopedIndex;
        if (!key) {
            throw new Error('scoped index or column should be set');
        }
        if (!extendSchemas[this.scopedTable]) {
            extendSchemas[this.scopedTable] = {}
        }
        extendSchemas[this.scopedTable][key] = {
            type, value
        };
    }
    protected setScopedColumn(column: string) {
        if (!column) {
            throw new Error(`setScopedColumn: column should not be empty ${column}`);
        }
        this.scopedIndex = undefined;
        this.scopedColumn = column;
    }
    protected setScopedIndex(index: string) {
        if (!index) {
            throw new Error(`setScopedIndex: index should not be empty ${index}`);
        }
        this.scopedIndex = `idx@${index}`;
        this.scopedColumn = undefined;
    }

    // AST Transformers
    // P_DDS: default
    // P_CREATE_TABLE: default
    protected P_CREATE_TABLE_COMMON(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        this.scopedTable = ast.table;
        return `CREATE TABLE ${ast.table} ` +
            `(${this.P_CREATE_TABLE_CREATE_DEFINITIONS(ast.columnsDef.def, extendSchemas)})` +
            `${this.P_CREATE_TABLE_OPTIONS(ast.tableOptions.def, extendSchemas)} ` +
            `${this.primaryKeyDefinitionHelper()};` +
            `${this.indexDefinitionsHelper()}`;
    }
    protected P_CREATE_TABLE_CREATE_DEFINITIONS(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return `${ast.map((d: any) => this.P_CREATE_TABLE_CREATE_DEFINITION(d.def, extendSchemas)).filter((e: string) => !!e).join(',')}`;
    }
    protected P_CREATE_TABLE_CREATE_DEFINITION(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        if (ast.column) {
            this.setScopedColumn(ast.column.name);
            return `${ast.column.name} ${this.O_DATATYPE(ast.column.def.datatype, extendSchemas)} ` +
                `${this.O_COLUMN_DEFINITION(ast.column.def.columnDefinition, extendSchemas)}`;
        } else if (ast.primaryKey) {
            this.primaryKeyColumns = this.primaryKeyColumns
            .concat(ast.primaryKey.columns.map((c: any) => {
                return {
                    name: c.def.column,
                    sort: c.def.sort
                }
            }));
        } else if (ast.index) {
            this.indices.push(this.indexHelper(ast.index, {unique:false, sparse:false}));
        } else if (ast.uniqueKey) {
            this.indices.push(this.indexHelper(ast.uniqueKey, {unique:true, sparse:false}));
        }
        return "";
    }
    protected P_ALTER_TABLE(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        this.scopedTable = ast.table;
        return `${ast.specs.map((spec: any) => this.P_ALTER_TABLE_SPECS(spec, extendSchemas)).join(';')}`;
    }
    protected P_ALTER_TABLE_SPECS(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return this.O_ALTER_TABLE_SPEC(ast.def.spec, extendSchemas);
    }
    protected P_CREATE_TABLE_OPTIONS(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return `${ast.map((cd: any) => this.P_CREATE_TABLE_OPTION(cd.def, extendSchemas)).join()}`
    }
    protected P_CREATE_TABLE_OPTION(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        if (ast.engine) {
            // InnoDB or MyISAM
        }
        return "";
    }
    protected P_RENAME_TABLE(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return ast.map((p: any) => `RENAME TABLE ${p.table} TO ${p.newName}`).join(';');
    }
    protected P_CREATE_INDEX(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        this.scopedTable = ast.table;
        return this.indexDefinitionHelper(this.indexHelper(ast, ast.type));
    }
    protected P_DROP_INDEX(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return `DROP INDEX ${ast.index}`;
    }
    protected P_DROP_TABLE(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return ast.map((t: string) => `DROP TABLE ${t}`).join(';');
    }

    protected O_ALTER_TABLE_SPEC(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        const actionSqlMap: {
            [action: string]: (ast: any, extendSchemas: SpannerExtendSchemaSources) => string
        } = {
            addColumn: this.O_ALTER_TABLE_SPEC_addColumn,
            dropColumn: this.O_ALTER_TABLE_SPEC_dropColumn,
            changeColumn: this.O_ALTER_TABLE_SPEC_changeColumn,
            addIndex: this.O_ALTER_TABLE_SPEC_addIndex,
            addFulltextIndex: this.O_ALTER_TABLE_SPEC_addIndex, //no fulltext index for spanner
            addSpatialIndex: this.O_ALTER_TABLE_SPEC_addSpatialIndex,
            addUniqueKey: this.O_ALTER_TABLE_SPEC_addUniqueKey,
            dropIndex: this.O_ALTER_TABLE_SPEC_dropIndex,
        };
        return actionSqlMap[ast.def.action].call(this, ast.def, extendSchemas);
    }
    protected O_ALTER_TABLE_SPEC_addColumn(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return `ALTER TABLE ${this.scopedTable} ADD COLUMN ${ast.name} ` +
            this.alterColumnDefinitionHelper(ast, ast.name, extendSchemas);
    }
    protected O_ALTER_TABLE_SPEC_dropColumn(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        this.setScopedColumn(ast.column);
        return `ALTER TABLE ${this.scopedTable} DROP COLUMN ${ast.column}`;
    }
    protected O_ALTER_TABLE_SPEC_changeColumn(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        if (ast.newName && ast.column !== ast.newName) {
            throw new Error(`changing column name ${ast.column} => ${ast.newName} is not supported `);
        }
        return `ALTER TABLE ${this.scopedTable} ALTER COLUMN ${ast.column} ` +
            this.alterColumnDefinitionHelper(ast, ast.column, extendSchemas);
    }
    protected O_ALTER_TABLE_SPEC_addIndex(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return this.indexDefinitionHelper(this.indexHelper(ast, {unique:false, sparse:false}));
    }
    protected O_ALTER_TABLE_SPEC_addSpatialIndex(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return this.indexDefinitionHelper(this.indexHelper(ast, {unique:false, sparse:true}));
    }
    protected O_ALTER_TABLE_SPEC_addUniqueKey(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return this.indexDefinitionHelper(this.indexHelper(ast, {unique:true, sparse:false}));
    }
    protected O_ALTER_TABLE_SPEC_dropIndex(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        return `DROP INDEX ${ast.index}`;
    }
    protected O_DATATYPE(ast: any, extendSchemas: SpannerExtendSchemaSources): string {
        // handle all O_XXXXX_DATATYPE
        const lengthTypeChecker = (ast: any) => {
            if (ast.datatype.indexOf("blob") >= 0 || ast.datatype.indexOf("binary") >= 0) {
                return `bytes(${ast.length})`;
            } else {
                return `string(${ast.length})`;
            }
        };
        const intTypeChecker = (ast: any) => {
            if (ast.datatype === 'tinyint') {
                return "bool";
            } else {
                return "int64";
            }
        }
        const dataTypeMap: { [id:string]:string|DataTypeChecker|undefined } = {
            O_INTEGER_DATATYPE: intTypeChecker,
            O_FIXED_POINT_DATATYPE: "float64",
            O_FLOATING_POINT_DATATYPE: "float64",
            O_BIT_DATATYPE: "bool",
            O_BOOLEAN_DATATYPE: "bool",
            O_DATETIME_DATATYPE: "timestamp",
            O_YEAR_DATATYPE: "date",
            O_VARIABLE_STRING_DATATYPE: lengthTypeChecker,
            O_FIXED_STRING_DATATYPE: lengthTypeChecker,
            O_ENUM_DATATYPE: "string(max)",
            O_SET_DATATYPE: undefined,
            O_SPATIAL_DATATYPE: undefined,
            O_JSON_DATATYPE: "string(max)"
        };
        const t = dataTypeMap[ast.def.id];
        if (!t) {
            throw new Error(`unsupported data type: ${ast.def.id}`);
        } else if (typeof(t) === "function") {
            this.scopedColumnType = t(ast.def.def);
        } else {
            this.scopedColumnType = t;
        }
        return this.scopedColumnType;
    }
    // O_XXXXX_DATATYPE: default (ignored)
    protected O_COLUMN_DEFINITION(asts: any, extendSchemas: SpannerExtendSchemaSources): string {
        if (!Array.isArray(asts)) {
            asts = [asts];
        }
        return asts.map((ast: any) => {
            const a = ast.def;
            if (a.nullable === true) {
                return ""; //spanner does not allow `NULL` to express nullable column. all column nullable by default.
            } else if (a.nullable === false) {
                return "NOT NULL";
            } else if (a.autoincrement) {
                this.addExtendSchema(extendSchemas, "generator", "increment");
            } else if (a.default !== undefined) {
                this.addExtendSchema(extendSchemas, "default", this.defaultValueEncoder(a.default));
            }
            return "";
        }).join(' ');
    }

    // helpers
    protected alterColumnDefinitionHelper(ast: any, columnName: string, extendSchemas: SpannerExtendSchemaSources): string {
        this.setScopedColumn(columnName);
        return `${this.O_DATATYPE(ast.datatype, extendSchemas)} ` +
        `${this.O_COLUMN_DEFINITION(ast.columnDefinition, extendSchemas)}` +
        (ast.position ? (ast.position.after ? `AFTER ${ast.position.after}` : "FIRST") : "");
    }
    protected primaryKeyDefinitionHelper(): string {
        return `PRIMARY KEY (${this.indexColumnsHelper(this.primaryKeyColumns)})`;
    }
    protected indexDefinitionsHelper(): string {
        return this.indices.map(idx => this.indexDefinitionHelper(idx)).join(';');
    }
    protected indexDefinitionHelper(idx: Index): string {
        return `CREATE ` +
                (typeof(idx.type) === "string" ? idx.type :
                `${[
                    idx.type.unique ? "UNIQUE" : undefined,
                    idx.type.sparse ? "NULL_FILTERED" : undefined,
                    "INDEX"
                ].filter((e: string) => !!e).join(' ')}`) +
                ` ${idx.name} ` +
                `ON ${idx.table}(${this.indexColumnsHelper(idx.columns)})`;
    }
    protected indexHelper(index: any, type: IndexType): Index {
        const columns = index.columns.map((idx: any) => {
            return { name: idx.def.column, sort: idx.def.sort}
        });
        const name = index.name || this.indexNameHelper(this.scopedTable, columns);
        this.setScopedIndex(name);
        return {
            name,
            type,
            table: this.scopedTable,
            columns
        };
    }
    protected indexNameHelper(table: string, columns: Column[]): string {
        return `${table}_idx_${columns.map((c: Column) => c.name).join('_')}`;
    }
    protected indexColumnsHelper(columns: Column[]): string {
        return columns.map(c => `${[c.name, c.sort].filter((e: string) => !!e).join(' ')}`).join(',');
    }
};

export function MakeDebug(t: SpannerDDLTransformer): SpannerDDLTransformer {
    return new Proxy(t, {
        get: (target: SpannerDDLTransformer, prop: string|number|symbol, receiver: any): any => {
            const origProp = Reflect.get(target, prop, receiver);
            if (typeof(origProp) === 'function') {
                return function (...args: any[]) {
                    console.log('-------------------------------------------');
                    console.log(prop, ...args);
                    return origProp.call(receiver, ...args);
                }
            } else {
                return origProp;
            }
        }
    });
}
