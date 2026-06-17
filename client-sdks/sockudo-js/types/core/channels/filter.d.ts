export interface FilterNode {
    op?: string;
    key?: string;
    cmp?: string;
    val?: string;
    vals?: string[];
    nodes?: FilterNode[];
}
export declare const Filter: {
    eq: (key: string, val: string) => FilterNode;
    neq: (key: string, val: string) => FilterNode;
    in: (key: string, vals: string[]) => FilterNode;
    nin: (key: string, vals: string[]) => FilterNode;
    exists: (key: string) => FilterNode;
    notExists: (key: string) => FilterNode;
    startsWith: (key: string, val: string) => FilterNode;
    endsWith: (key: string, val: string) => FilterNode;
    contains: (key: string, val: string) => FilterNode;
    gt: (key: string, val: string) => FilterNode;
    gte: (key: string, val: string) => FilterNode;
    lt: (key: string, val: string) => FilterNode;
    lte: (key: string, val: string) => FilterNode;
    and: (...nodes: FilterNode[]) => FilterNode;
    or: (...nodes: FilterNode[]) => FilterNode;
    not: (node: FilterNode) => FilterNode;
};
export declare function validateFilter(filter: FilterNode): string | null;
export declare const FilterExamples: {
    eventType: (type: string) => FilterNode;
    eventTypes: (types: string[]) => FilterNode;
    range: (key: string, min: string, max: string) => FilterNode;
    importantEvents: (xGThreshold: string) => FilterNode;
};
