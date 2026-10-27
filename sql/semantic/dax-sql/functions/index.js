// The function library: name -> (compiler, args, env, ast) => ir.
//   SCALAR     functions that return a value
//   TABLE      functions that return a table
//   MODIFIERS  what a function means as a filter argument of CALCULATE when that is not a
//              filter (ALL removes filters, USERELATIONSHIP changes a relationship)
import * as scalarFns from './scalar.js?v=52cdc75';
import * as aggregateFns from './aggregate.js?v=52cdc75';
import * as tableFns from './table.js?v=52cdc75';
import * as timeFns from './time.js?v=52cdc75';
import * as windowFns from './window.js?v=52cdc75';
import * as visualFns from './visual.js?v=52cdc75';

export const SCALAR = new Map(Object.entries({ ...scalarFns.scalar, ...aggregateFns.scalar, ...timeFns.scalar, ...windowFns.scalar, ...visualFns.scalar }));
export const TABLE = new Map(Object.entries({ ...tableFns.table, ...timeFns.table, ...windowFns.table }));
export const MODIFIERS = new Map(Object.entries(tableFns.modifiers));
