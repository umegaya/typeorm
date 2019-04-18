import {Table} from "../../schema-builder/table/Table";
import {TableColumn} from "../../schema-builder/table/TableColumn";

export interface SpannerExtendColumnSchema {
    default?: any;
    generatorStorategy?: "uuid"|"increment";
    generator?: () => any;
}
export interface SpannerExtendSchemas {
    [table: string]: {
        [column: string]: SpannerExtendColumnSchema;
    }
}
export interface SpannerExtendColumnSchemaSource {
    type: string;
    value: string;
}
export interface SpannerExtendSchemaSources {
    [table: string]: {
        [column: string]: SpannerExtendColumnSchemaSource;
    }
}

export interface SpannerDatabase {
    handle: any;
    tables: {
        [key: string]: Table;
    }
    /**
     * extra schema information
     */
    schemas: SpannerExtendSchemas | null;
}

export interface SpannerExtendedColumnProps {
    databaseName: string;
    default?: any;
    generationStrategy?: "uuid" | "increment";
}

export class SpannerExtendedColumnPropsFromTableColumn implements SpannerExtendedColumnProps {
    databaseName: string;
    constructor(c: TableColumn) {
        this.databaseName = c.name;
        Object.assign(this, c);
    }
}

export interface SpannerExtendedTableProps {
    name: string;
    columns: SpannerExtendedColumnProps[];
}
