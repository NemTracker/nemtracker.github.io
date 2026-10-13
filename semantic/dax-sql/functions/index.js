// The function library: name -> (compiler, args, env, ast) => ir.
//   SCALAR     functions that return a value
//   TABLE      functions that return a table
//   MODIFIERS  what a function means as a filter argument of CALCULATE when that is not a
//              filter (ALL removes filters, USERELATIONSHIP changes a relationship)
import * as scalarFns from './scalar.js?v=c1c610e';
import * as aggregateFns from './aggregate.js?v=c1c610e';
import * as tableFns from './table.js?v=c1c610e';
import * as timeFns from './time.js?v=c1c610e';
import * as windowFns from './window.js?v=c1c610e';
import * as visualFns from './visual.js?v=c1c610e';

export const SCALAR = new Map(Object.entries({ ...scalarFns.scalar, ...aggregateFns.scalar, ...timeFns.scalar, ...windowFns.scalar, ...visualFns.scalar }));
export const TABLE = new Map(Object.entries({ ...tableFns.table, ...timeFns.table, ...windowFns.table }));
export const MODIFIERS = new Map(Object.entries(tableFns.modifiers));
