import {BaseConnectionOptions} from "../../connection/BaseConnectionOptions";

/**
 * Spanner-specific connection options.
 */
export interface SpannerConnectionOptions extends BaseConnectionOptions {

    /**
     * Database type.
     */
    readonly type: "spanner";

    /**
     * project id which holds spanner instances.
     */
    readonly projectId: string;

    /**
     * instance id of spanner node.
     */
    readonly instanceId: string;

    /**
     * spanner database id 
     */
    readonly databaseId: string;

}