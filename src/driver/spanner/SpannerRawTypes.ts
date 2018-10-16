import {Table} from "../../schema-builder/table/Table";

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
