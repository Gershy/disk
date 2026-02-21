import { isCls, inCls, getClsName, skip } from '@gershy/clearing';

type MaybePromise<T> = T | Promise<T>;
export const then = (val: MaybePromise<any>, rsv = (v => v), rjc = ((e): any => { throw e; })) => {
  
  // Act on `val` regardless of whether it's a Promise or immediate value; return `rsv(val)`
  // either immediately or as a Promise
  
  // Promises are returned with `then`/`fail` handling
  if (inCls(val, Promise)) return val.then(rsv).catch(rjc);
  
  try        { return rsv(val); }
  catch(err) { return rjc(err); }
  
};
export const safe = (fn: () => MaybePromise<any>, rjc = ((e): any => { throw e; })) => {
  try        { return then(fn(), v => v, rjc); }
  catch(err) { return rjc(err); }
};