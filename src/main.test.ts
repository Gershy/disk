import { assertEqual } from '../build/utils.test.ts';
import { Ent } from './setup.ts';
import { rootEnt } from './main.ts';

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
        
        await ent.kid('val').setData('hello');
        const val = await ent.kid('val').getData('str');
        
        assertEqual(val, 'hello');
        
      })
    },
    {
      name: 'basic json data',
      fn: () => isolated(async ent => {
        
        await ent.kid('val').setData({ a: 'b', x: 'y' });
        const val = await ent.kid('val').getData('json');
        
        assertEqual(val, { a: 'b', x: 'y' });
        
      })
    },
    {
      name: 'basic overwrite',
      fn: () => isolated(async ent => {
        
        await Promise.all([
          
          ent.kid('val').setData({ a: 1, b: 2 }),
          ent.kid('val').setData({ a: 1, b: 3 }),
          ent.kid('val').setData({ a: 1, b: 4 }),
          ent.kid('val').setData({ a: 1, b: 5 }),
          ent.kid('val').setData({ a: 1, b: 6 }),
          
        ]);
        
        const val = await ent.kid('val').getData('json');
        
        assertEqual(val, { a: 1, b: 6 });
        
      })
    },
    {
      name: 'leaf to node conversion',
      fn: () => isolated(async ent => {
        
        await ent.kid('par')    .setData({ desc: 'par node' });
        await ent.kid('par/kid').setData({ desc: 'kid node' });
        
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
        const valPrm = ent.kid('data').getData('str');
        await sleep(5);
        
        headStream.write('111');
        await sleep(5);
        
        headStream.write('222');
        await sleep(5);
        
        headStream.write('333');
        headStream.end();
        await sleep(5);
        
        const val = await valPrm;
        
        assertEqual(val, '111222333');
        
      })
    },
    {
      name: 'data tail stream',
      fn: () => isolated(async ent => {
        
        await ent.kid('data').setData('abc'.repeat(1000));
        
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
    },
    {
      name: 'encoding',
      fn: () => isolated(async ent => {
        
        const dataEnt = ent.kid('data');
        
        const assertEmptyAllEncodings = async () => {
          
          const vals = await Promise[allObj]({
            str:  dataEnt.getData('str'),
            bin:  dataEnt.getData('bin'),
            json: dataEnt.getData('json')
          });
          assertEqual(vals, {
            str:  '',
            bin:  Buffer.alloc(0),
            json: null
          });
          
        };
        
        await assertEmptyAllEncodings();
        
        await dataEnt.setData('[1,2,3,{"x":"y"}]');
        assertEqual(
          await Promise[allObj]({
            str:  dataEnt.getData('str'),
            bin:  dataEnt.getData('bin'),
            json: dataEnt.getData('json')
          }),
          {
            str: '[1,2,3,{"x":"y"}]',
            bin: Buffer.from('[1,2,3,{"x":"y"}]'),
            json: [ 1, 2, 3, { x: 'y' } ]
          }
        );
        
        await dataEnt.setData('');
        await assertEmptyAllEncodings();
        
        await dataEnt.setData(Buffer.from([ 0, 1, 2, 3, 4, 5 ]));
        assertEqual(
          await Promise[allObj]({
            str:  dataEnt.getData('str'),
            bin:  dataEnt.getData('bin'),
            json: dataEnt.getData('json').then(
              val => ({ success: true, val }),
              err => ({ success: false, err })
            )
          }),
          {
            str: '\u0000\u0001\u0002\u0003\u0004\u0005',
            bin: Buffer.from([ 0, 1, 2, 3, 4, 5 ]),
            json: {
              success: false,
              err: Error('Failed locked op: "getData"')[mod]({
                cause: new Error('non-json')
              })
            }
          }
        );
        
        await dataEnt.setData(Buffer.alloc(0));
        await assertEmptyAllEncodings();
        
        await dataEnt.setData('hellooo');
        assertEqual(
          await dataEnt.getData('bin'),
          Buffer.from('hellooo')
        );
        
        await dataEnt.setData(null);
        await assertEmptyAllEncodings();
        
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