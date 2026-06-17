// Separate entry point for Filter exports
// This allows clean imports: import { Filter } from '@sockudo/client/filter'

export { Filter, FilterExamples, validateFilter } from "./core/channels/filter";
export type { FilterNode } from "./core/channels/filter";
