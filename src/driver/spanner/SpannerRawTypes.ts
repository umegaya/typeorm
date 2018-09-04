import {Table} from "../../schema-builder/table/Table";

export interface SpannerDatabase {
    handle: any;
    tables: {
        [key:string]:Table;
    }
}
