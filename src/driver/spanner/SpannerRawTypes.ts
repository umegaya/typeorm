import {Table} from "../../schema-builder/table/Table";

export interface SpannerExtendSchemas {
    [table: string]: {
        [column: string]: {
            default?: any;
            generatorStorategy?: "uuid"|"increment";
            generator?: () => any;
        }
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
