import { assertEqual } from '../build/utils.test.ts';
import { rootEnt, Ent } from './main.ts';

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