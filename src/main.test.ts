import { assertEqual } from '../build/utils.test.ts';
import { rootEnt, Ent } from './main.ts';

// Type testing
(async () => {
  
  type Enforce<Provided, Expected extends Provided> = { provided: Provided, expected: Expected };
  
  type Tests = {
    1: Enforce<{ x: 'y' }, { x: 'y' }>,
  };
  
})();

// Test cases
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
  const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
  
  const cases = [
    
    {
      name: 'basic string data',
      fn: () => isolated(async ent => {
        
        await ent.kid('val').setData('hello', 'utf8');
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
    },
    {
      name: 'data head stream',
      fn: () => isolated(async ent => {
        
        const headStream = await ent.kid('data').getDataHeadStream();
        
        // Note this read is expected to wait for the head stream to be ended
        const valPrm = ent.kid('data').getData('utf8');
        await sleep(5);
        
        headStream.write('111');
        await sleep(5);
        
        headStream.write('222');
        await sleep(5);
        
        headStream.write('333');
        headStream.end();
        
        const val = await valPrm;
        
        assertEqual(val, '111222333');
        
      })
    },
    {
      name: 'data tail stream',
      fn: () => isolated(async ent => {
        
        await ent.kid('data').setData('abc'.repeat(1000), 'utf8');
        
        const readStream = await ent.kid('data').getDataTailStream();
        const chunks: any[] = [];
        readStream.on('data', d => chunks.push(d));
        await readStream.prm;
        
        assertEqual(Buffer.concat(chunks).toString('utf8'), 'abc'.repeat(1000));
        
      })
    },
    {
      name: 'get kids',
      fn: () => isolated(async ent => {
        
        await Promise.all(
          (50)[toArr](v => ent.kid(`par/kid${v}`).setData(v.toString(10)))
        );
        
        const kids = await ent.kid('par').getKids();
        assertEqual(kids[map](kid => kid.toString()), (50)[toObj](v => [ `kid${v}`, `${ent.toString()}/par/kid${v}` ]));
        
      })
    },
    {
      name: 'iterate kids',
      fn: () => isolated(async ent => {
        
        await Promise.all(
          (50)[toArr](v => ent.kid(`par/kid${v}`).setData(v.toString(10)))
        );
        
        const kids: Ent[] = [];
        for await (const kid of await ent.kid('par').kids())
          // Note there are no guarantees for iteration order
          kids.push(kid);
        
        assertEqual(
          new Set(kids[map](kid => kid.toString())),
          new Set((50)[toArr](v => `${ent.toString()}/par/kid${v}`))
        );
        
      })
    },
    {
      name: 'iterate kids with interrupt',
      fn: () => isolated(async ent => {
        
        await Promise.all(
          (50)[toArr](v => ent.kid(`par/kid${v}`).setData(v.toString(10)))
        );
        
        const kids: Ent[] = [];
        const kidIt = await ent.kid('par').kids();
        let cnt = 0;
        for await (const kid of kidIt) {
          kids.push(kid);
          if (++cnt >= 30) break;
        }
        await kidIt.close();
        
        // Note there are no guarantees for iteration order - comparing size only
        assertEqual(kids.length, 30);
        
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