import fs from 'node:fs';
import path from 'node:path';
import { inCls, getCls, getClsName } from '@gershy/clearing';
import { rootEnt, Ent } from './main.ts';

const equal = (v0: any, v1: any, path: (string | number)[] = []): { equal: true } | { equal: false, path: (string | number)[], [K: string]: any } => {
  
  if (v0 === v1)                return { equal: true };
  if (v0 == null || v1 == null) return { equal: false, path, reason: 'identity', v0, v1 };
  
  const cls0 = getCls(v0);
  const cls1 = getCls(v1);
  
  if (cls0 !== cls1)    return { equal: false, path, reason: 'class', cls0: getClsName(v0), cls1: getClsName(v1) };
  if (cls0 === null)    return { equal: false, path, reason: 'identity', v0, v1 };
  if (cls0 === String)  return { equal: false, path, reason: 'identity', v0, v1 };
  if (cls0 === Number)  return { equal: false, path, reason: 'identity', v0, v1 };
  if (cls0 === Boolean) return { equal: false, path, reason: 'identity', v0, v1 };
  
  if (cls0 === Array) {
    
    const len0 = v0[count]();
    const len1 = v1[count]();
    if (len0 !== len1) return { equal: false, path, reason: 'arr count', len0, len1 };
    
    for (let i = 0; i < len0; i++) {
      const eq = equal(v0[i], v1[i], [ ...path, i ]);
      if (!eq.equal) return eq;
    }
    
    return { equal: true };
    
  }
  
  if (cls0 === Object) {
    
    const len0 = v0[count]();
    const len1 = v1[count]();
    if (len0 !== len1) return { equal: false, path, reason: 'obj count', len0, len1 };
    
    for (const k in v0) {
      if (!v1[has](k)) return { equal: false, path: [ ...path, k ], reason: 'obj key', key: k, obj0: 'present', obj1: 'absent' } ;
      
      const eq = equal(v0[k], v1[k], [ ...path, k ]);
      if (!eq.equal) return eq;
      
    }
    return { equal: true };
    
  }
  
  if (cls0 === Set) {
    
    if (v0.size !== v1.size) return { equal: false, path, reason: 'set count', len0: v0.size, len1: v1.size };
    for (const v of v0)
      if (!v1.has(v))
        return { equal: false, path, reason: 'set inclusion', val: v, set0: 'present', set1: 'absent' };
    
    return { equal: true };
    
  }
  
  if (cls0 === Map) {
    
    if (v0.size !== v1.size) return { equal: false, path, reason: 'map count', len0: v0.size, len1: v1.size };
    
    for (const [ k, v ] of v0) {
      if (!v1.has(k)) return { equal: false, path: [ ...path, k ], reason: 'map key', key: k, map0: 'present', map1: 'absent' };
      
      const eq = equal(v, v1.get(k), [ ...path, k ]);
      if (!eq.equal) return eq;
    }
    
    return { equal: true };
    
  }
  
  if (inCls(v0, Error)) {
    // Include message, but not stack (because it's a nightmare to define expected stacktrace
    // values when defining expected results)
    return equal({ $msg: v0.message, ...v0 }, { $msg: v1.message, ...v1 }, [ ...path, '<convertToObj>' ]);
  }
  
  return { equal: false, path, reason: 'unknown comparison', cls: getClsName(v0) };
  
};
const assertEqual = (v0: any, v1: any) => {
  
  const { equal: eq, ...props } = equal(v0, v1);
  
  if (!eq) throw Error('assert equal')[mod]({ ...props });
  
};

// Type testing
(async () => {
  
  type Enforce<Provided, Expected extends Provided> = { provided: Provided, expected: Expected };
  
  type Tests = {
    1: Enforce<{ x: 'y' }, { x: 'y' }>,
  };
  
})();

(async () => {
  
  const isolated = async (fn: (ent: Ent) => Promise<void>) => {
    
    let ent: null | Ent = null;
    try {
      
      ent = await rootEnt.kid([ import.meta.dirname, '.isolatedTest' ], { newTx: true });
      await fn(ent);
      
    } finally {
      
      await ent?.rem();
      ent?.tx.end();
      
    }
    
  };
  
  const cases = [
    
    {
      name: 'basic string data',
      fn: () => isolated(async ent => {
        
        await ent.kid('val').setData('hello');
        const val = await ent.kid('val').getData('utf8');
        
        assertEqual(val, 'hello');
        
      })
    },
    {
      name: 'basic json data',
      fn: () => isolated(async ent => {
        
        await ent.kid('val').setData({ a: 'b', x: 'y' }, 'json');
        const val = await ent.kid('val').getData('json');
        
        assertEqual(val, { a: 'b', x: 'y' });
        
      })
    },
    {
      name: 'basic overwrite',
      fn: () => isolated(async ent => {
        
        await Promise.all([
          
          ent.kid('val').setData({ a: 1, b: 2 }, 'json'),
          ent.kid('val').setData({ a: 1, b: 3 }, 'json'),
          ent.kid('val').setData({ a: 1, b: 4 }, 'json'),
          ent.kid('val').setData({ a: 1, b: 5 }, 'json'),
          ent.kid('val').setData({ a: 1, b: 6 }, 'json'),
          
        ]);
        
        const val = await ent.kid('val').getData('json');
        
        assertEqual(val, { a: 1, b: 6 });
        
      })
    },
    {
      name: 'leaf to node conversion',
      fn: () => isolated(async ent => {
        
        await ent.kid('par')    .setData({ desc: 'par node' }, 'json');
        await ent.kid('par/kid').setData({ desc: 'kid node' }, 'json');
        
        const parData = await ent.kid('par').getData('json');
        const kidData = await ent.kid('par/kid').getData('json');
        
        assertEqual({ parData, kidData }, {
          parData: { desc: 'par node' },
          kidData: { desc: 'kid node' }
        });
        
      })
    }
    
  ];
  for (const { name, fn } of cases) {
    
    try {
      
      await fn();
      
    } catch (err: any) {
      
      console.log(`FAILED: "${name}"`, err[limn]());
      process.exit(1);
      
    }
    
  }
  
  console.log(`Passed ${cases.length} test${cases.length === 1 ? '' : 's'}`);
  
})();