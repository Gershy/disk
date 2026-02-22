import nodePath, { PlatformPath } from 'node:path';
import nodeFs, { Stats } from 'node:fs';
import { Readable, Writable } from 'node:stream';
import { isCls, inCls, getClsName, skip } from '@gershy/clearing';
import retry from '@gershy/retry';
import * as utils from './utils.ts';

type Lock = { type: string, fp: Fp, prm: PromiseLater };
type LineageLock = { fp: Fp, prm: PromiseLater };
type PartialIterator<T> = {
  [Symbol.asyncIterator](): AsyncGenerator<T, any, any>,
  close():                  Promise<void>,
  prm:                      Promise<void>
};

const fs = (() => {
  
  type FsPrm = (typeof nodeFs)['promises'] & Pick<typeof nodeFs, 'createReadStream' | 'createWriteStream'>;
  return ({ ...nodeFs.promises, ...nodeFs[slice]([ 'createReadStream', 'createWriteStream' ]) } as FsPrm)
  
    [map]((fsVal: any, name): any => {
      
      if (!inCls(fsVal, Function)) return fsVal;
      
      return (...args) => {
        
        const err = Error();
        return utils.safe(
          () => fsVal(...args),
          cause => err[fire]({ cause, msg: `Failed low-level ${name} on "${args[0]}"` })
        );
        
      };
      
    }) as FsPrm;
  
})();

export class Fp {
  
  // alphanum!@ followed by the same including ".", "-" (careful with "-" in regexes), "_", and " "
  static validComponentRegex = /^[a-zA-Z0-9!@][-a-zA-Z0-9!@._ ]*$/;
  
  // All components must have a char that isn't "." (we allow "~" at this level - FilesysTransaction manages this char)
  static illegalComponentRegex = /^[.]+$/;
  
  private path: PlatformPath;
  public cmps: string[];
  public fspVal: null | string;
  constructor(vals: string | string[], path=nodePath) {
    
    if (!isCls(vals, Array)) vals = [ vals ];
    
    vals = vals
      [map](cmp => cmp.split(/[/\\]+/)) // Each String is broken into its components
      .flat(1);                         // Finally flatten into flat list of components
    
    const illegalCmp = vals[find](val => Fp.illegalComponentRegex.test(val)).val;
    if (illegalCmp) throw Error('illegal file component provided')[mod]({ cmps: vals, illegalCmp });
    
    // Use `path.resolve`; first component being "/" ensures working directory is always ignored;
    // split final result by "/" and "\" which may produce an empty leading item on posix as, e.g.,
    // `'/a/b/c'.split('/') === [ '', 'a', 'b', 'c' ]`
    
    this.path = path;
    this.cmps = path.resolve('/', ...vals).split(/[/\\]+/)[map](v => v || skip);
    this.fspVal = null;
    
  }
  
  toString() { return this.cmps.length ? `file://${this.cmps.join('/')}` : `file://${this.path.resolve('/').split(/[/\\]/).filter(Boolean).join('/')}`; }
  count() { return this.cmps.length; }
  kid(fp: string | string[]) { return new Fp([ this.cmps, fp ].flat(1)); }
  sib(cmp: string) { return new Fp([ this.cmps.slice(0, -1), cmp ].flat(1)); }
  par(n=1) { return (n <= 0) ? this : new Fp(this.cmps.slice(0, -n)); }
  contains(fp) { return this.cmps.length  <= fp.cmps.length && this.cmps.every((v, i) => fp.cmps[i] === v); }
  equals(fp)   { return this.cmps.length === fp.cmps.length && this.cmps.every((v, i) => fp.cmps[i] === v); }
  fsp() { // "file system pointer"
    
    if (!this.fspVal) {
      let fspVal = this.path.resolve('/', ...this.cmps);
      if (/^[A-Z][:]$/.test(fspVal)) fspVal += '\\';
      /// {ASSERT=
      if (!/^([A-Z]+[:])?[/\\]/.test(fspVal)) throw Error('path doesn\'t start with optional drive indicator (e.g. "C:") followed by "/" or "\\"')[mod]({ fp: this, fsp: fspVal });
      /// =ASSERT}
      this.fspVal = fspVal;
    }
    return this.fspVal;
    
  }
  relativeCmps(trg: Fp) {
    
    // Returns an array of components which, after traversing, result in `fp`; note ".." is used to
    // indicate parent nodes!
    
    const [ srcCmps, trgCmps ] = [ this.cmps, trg.cmps ];
    
    const minLen = Math.min(srcCmps.length, trgCmps.length);
    
    let numCommon = 0;
    while (numCommon < minLen && srcCmps[numCommon] === trgCmps[numCommon]) numCommon++;
    
    const numPars = srcCmps.length - numCommon; // Traverse back to the common node
    const remaining = trgCmps.slice(numCommon); // Traverse forward to the target
    
    return [ ...(numPars)[toArr](() => '..'), ...remaining ];
    
  }
  * getLineage(fp: Fp) {
    
    // Yield every Filepath from `this` up to (excluding) `fp`
    if (!this.contains(fp)) throw Error('provided Filepath isn\'t a child');
    
    let ptr: Fp = this;
    while (!ptr.equals(fp)) {
      yield ptr;
      ptr = ptr.kid(fp.cmps[ptr.count()]);
    }
    
  }
  
};

export interface AbstractSys {
  
  safeStat: (fp: Fp) => Promise<null | Stats>,
  getType: (fp: Fp) => Promise<null | 'leaf' | 'node'>,
  swapLeafToNode: (fp: Fp, opts?: { tmpCmp?: string }) => Promise<void>,
  ensureNode: (fp: Fp, opts?: { earliestUncertainFp?: Fp }) => Promise<void>,
  ensureLineageLocks: (lineageLocks: LineageLock[]) => Promise<void>,
  getData: {
    (fp: Fp):                             Promise<Buffer>,
    (fp: Fp, opts: null):                 Promise<Buffer>,
    (fp: Fp, opts: {}):                   Promise<Buffer>,
    (fp: Fp, opts: { encoding: null }):   Promise<Buffer>,
    (fp: Fp, opts: string):               Promise<string>,
    (fp: Fp, opts: { encoding: string }): Promise<string>,
  },
  setData: (lineageLocks: LineageLock[], fp: Fp, data: string | Buffer) => Promise<void>,
  remSubtree: (fp: Fp) => Promise<void>,
  getKidCmps: (fp: Fp) => Promise<string[]>,
  remEmptyAncestors: (fp: Fp) => Promise<void>,
  remNode: (fp: Fp) => Promise<void>,
  getDataSetterStreamAndFinalizePrm: (lineageLocks: LineageLock[], fp: Fp) => Promise<{ stream: Writable, prm: Promise<void> }>,
  getDataGetterStreamAndFinalizePrm: (fp: Fp) => Promise<{ stream: Readable, prm: Promise<void> }>,
  getKidIteratorAndFinalizePrm: (fp: Fp, opts?: { map: Fn, bufferSize: number }) => Promise<PartialIterator<string>>
  
};
export class FileSys implements AbstractSys {
  
  static defaultDataCmp =  '~';
  static getTmpCmp = () => `~${(Number[int32] * Math.random())[toStr](String[base32], 7)}`;
  
  constructor() {}
  async safeStat(fp: Fp) {
    try              { return await fs.stat(fp.fsp()); }
    catch(err: any) { if (err.code !== 'ENOENT') throw err; }
    return null;
  }
  async getType(fp: Fp) {
    
    const stat = await this.safeStat(fp);
    if (stat === null)      return null;
    if (stat.isFile())      return 'leaf';
    if (stat.isDirectory()) return 'node';
    
    throw Error('unexpected filesystem entity')[mod]({ stat });
    
  }
  async swapLeafToNode(fp: Fp, { tmpCmp=FileSys.getTmpCmp() }={}) {
    
    // We want a dir to replace an existing file (without reads on that previously existing file to
    // fail) - so we replace the file with a directory containing a "default data file"
    
    // Basically we know that `fp` is a leaf, and we want it to become a node, with
    // `fp.kid(this.constructor.defaultDataCmp)` holding the data previously at `fp`
    
    const fsp = fp.fsp();                              // Path to original file
    const tmpFsp = fp.sib(tmpCmp).fsp();               // Path to temporary file (sibling of original file)
    const valFsp = fp.kid(FileSys.defaultDataCmp).fsp(); // Path to final file
    
    await fs.rename(fsp, tmpFsp);    // Move file out of the way
    await fs.mkdir(fsp);             // Set directory where file used to be
    await fs.rename(tmpFsp, valFsp); // Set original file as "default data file"
    
  }
  async ensureNode(fp: Fp, { earliestUncertainFp=new Fp([]) }={}) {
    
    // Ensure all ancestor nodes up to (but excluding) `fp`; overall ensures that an entity can be
    // written at `fp`. It doesn't touch `fp`, only `fp`'s ancestors!
    
    let ptr = earliestUncertainFp;
    while (!ptr.equals(fp)) {
      
      const type = await this.getType(ptr);
      
      // If nothing exists create dir; if file exists swap it to dir
      if      (type === null)   await fs.mkdir(ptr.fsp());
      else if (type === 'leaf') await this.swapLeafToNode(ptr);
      
      // Extend `ptr` with the next component in `fp`
      ptr = ptr.kid(fp.cmps[ptr.count()]);
      
    }
    
  }
  async ensureLineageLocks(lineageLocks: LineageLock[]) {
    
    // Note that `ensureNode` isn't being used, since the context always wants to be able to
    // resolve lineage-locks asap, and `ensureNode` doesn't expose a way to do this
    
    if (lineageLocks[empty]()) return;
    
    let lastFp: Fp = lineageLocks[0].fp;
    for (const { fp, prm } of lineageLocks) {
      
      if (!lastFp.contains(fp)) throw Error('Invalid lineage');
      lastFp = fp;
      
      const type = await this.getType(fp);
      if (type === null)        await fs.mkdir(fp.fsp());
      else if (type === 'leaf') await this.swapLeafToNode(fp);
      
      prm.resolve();
      
    }
    
  }
  async remEmptyAncestors(fp: Fp) {
    
    // The passed `fp` should be the first potentially empty *directory* - do not pass a file!
    
    while (true) {
      
      const dir = await fs.readdir(fp.fsp()).catch(err => {
        if (err.code === 'ENOENT') return [] as string[];
        throw err;
      });
      
      // Stop as soon as we encounter a non-empty directory; note an empty "~" file doesn't count!
      if (dir.length === 1 && dir[0] === '~') {
        // If the only child is an empty "~" node, delete it and continue...
        const stat = await this.safeStat(fp.kid('~'))
        if (stat?.size) break;
        await this.remNode(fp.kid('~'));
      } else if (dir.length) {
        break;
      }
      
      // Remove any empty directories
      await retry({
        attempts: 5,
        opts: { delay: n => n * 50 },
        fn: () => fs.rmdir(fp.fsp()).catch(err => {
          if (err.code === 'ENOENT')    return; // Success - nonexistence is the desired state!
          if (err.code === 'ENOTEMPTY') return; // Success - dir is non-empty, so no work to do
          if (err.code === 'EPERM')     throw err[mod]({ retry: true }); // Retry on EPERM
          throw err;
        })
      });
      
      fp = fp.par();
      
    }
    
  }
  async remNode(fp: Fp) {
    
    try              { await fs.unlink(fp.fsp()); }
    catch(err: any) { if (err.code !== 'ENOENT') throw err; }
    
  }
  async setData(lineageLocks: LineageLock[], fp: Fp, data: string | Buffer) {
    
    const type = await this.getType(fp);
    
    if (type === null) {
      
      // Ensure lineage; once this loop is over we know `fp.par()`
      // certainly exists, and `fp` itself doesn't
      await this.ensureLineageLocks(lineageLocks);
      await fs.writeFile(fp.fsp(), data);
      
    } else {
      
      // `fp` is pre-existing! immediately resolve all lineage locks and simply write to either the
      // plain file or "~" kid
      
      // All lineage locks are released; we don't touch the lineage!
      for (const { prm } of lineageLocks) prm.resolve();
      
      const fsp = type === 'node' ? nodePath.join(fp.fsp(), '~') : fp.fsp();
      await fs.writeFile(fsp, data);
      
    }
    
  }
  async getData(fp: Fp, opts: string | null | { encoding?: string | null }=null) {
    
    if (!isCls(opts, Object)) opts = { encoding: opts ?? null };
    const { encoding: enc } = opts;
    
    const fsp = fp.fsp();
    const type = await this.getType(fp);
    const emptyVal = (enc ? '' : Buffer.alloc(0)) as any;
    
    switch (type) {
      
      case null:
        return emptyVal;
      
      case 'leaf':
        return fs.readFile(fsp, opts as any).catch(err => {
          if (err.code === 'ENOENT') return emptyVal;
          throw err;
        });
      
      case 'node':
        return fs.readFile(nodePath.join(fsp, '~'), opts as any).catch(err => {
          if (err.code === 'ENOENT') return emptyVal;
          throw err;
        });
      
    }
    
  }
  async remSubtree(fp: Fp) {
    
    await retry({
      attempts: 5,
      opts: { delay: n => n * 50 },
      fn: n => {
        return fs.rm(fp.fsp(), { recursive: true }).catch(err => {
          if (err.code === 'ENOENT') return;                         // Success - nonexistence is the desired state
          if (err.code === 'EPERM')  throw err[mod]({ retry: true }); // Retry on EPERM
          throw err;
        })
      }
    });
    
    await this.remEmptyAncestors(fp.par());
    
  }
  async getKidCmps(fp: Fp): Promise<string[]> {
    
    return fs.readdir(fp.fsp())
      .then(cmps => cmps.filter(cmp => cmp !== '~'))
      .catch(err => {
        if (err.code === 'ENOENT') return []; // No kids for non-existing entity
        if (err.code === 'ENOTDIR') return []; // The user passed a non-directory
        throw err;
      });
    
  }
  async getDataSetterStreamAndFinalizePrm(lineageLocks: LineageLock[], fp: Fp) {
    
    await this.ensureLineageLocks(lineageLocks);
    
    const stream = fs.createWriteStream(fp.fsp());
    const prm = new Promise<void>((rsv, rjc) => { stream.on('close', rsv); stream.on('error', rjc); });
    return { stream, prm };
    
  }
  async getDataGetterStreamAndFinalizePrm(fp: Fp) {
    
    const type = await this.getType(fp);
    
    if (type === null) {
      
      const nullReadable = new Readable();
      
      (async () => {
        nullReadable.push(null);
      })();
      
      return { stream: nullReadable, prm: Promise.resolve() };
      
    }
    
    if (type === 'leaf') {
      
      const err = Error();
      const stream = fs.createReadStream(fp.fsp());
      const prm = new Promise<void>((rsv, rjc) => {
        stream.on('close', rsv);
        stream.on('error', (cause: any) => {
          
          // ENOENT indicates the stream should return no data, successfully
          if (cause.code === 'ENOENT') return rsv();
          
          // ERR_STREAM_PREMATURE_CLOSE unwantedly propagates to the top-level; it should reject
          // like any other error, but need to:
          // 1. Suppress to prevent top-level crash
          // 2. Wrap in a separate error which is then thrown; this ensures the error will crash
          //    at the top-level if it is unhandled
          if (cause.code === 'ERR_STREAM_PREMATURE_CLOSE') {
            cause.suppress();
            cause = err[mod]({ msg: 'broken stream likely caused by unexpected client socket disruption', cause });
          }
          
          return rjc(cause);
          
        });
      });
      
      return { stream, prm };
      
    }
    
    if (type === 'node') {
      
      // Try recursing into the "~" dir
      return this.getDataGetterStreamAndFinalizePrm(fp.kid('~'));
      
    }
    
    throw Error('invalid type')[mod]({ fp, type });
    
  }
  async getKidIteratorAndFinalizePrm(fp: Fp, { bufferSize=150 }={} as { bufferSize?: number }) {
    
    const dir = await fs.opendir(fp.fsp(), { bufferSize }).catch(err => {
      if (err.code === 'ENOENT') return null; // `null` represents no children
      throw err;
    });
    
    if (!dir) return {
      // No yields; immediate completion
      async* [Symbol.asyncIterator](): AsyncGenerator<string> {},
      async close() {},
      prm: Promise.resolve()
    };
    
    const prm = Promise[later]<void>();
    return {
      async* [Symbol.asyncIterator](): AsyncGenerator<string> {
        
        for await (const ent of dir)
          if (ent.name !== '~')
            yield ent.name
        prm.resolve();
        
      },
      async close() {
        
        await new Promise<void>((rsv, rjc) => dir.close(err => err ? rjc(err) : rsv()))
          .catch(err => {
            if (err.code === 'ERR_DIR_CLOSED') return; // Tolerate this error; it indicates multiple attempts to close, which is fine!
            throw err;
          })
          .then(prm.resolve, prm.reject);
        
      },
      prm
    };
    
  }
  
};

export class Tx {
  
  public fp: Fp;
  private provider: FileSys;
  private locks: Set<Lock>;
  private active: boolean;
  private endFns: Array<(...args: any[]) => any>;
  
  constructor(fp: Fp | string | string[] = []) {
    
    this.fp = isCls(fp, Fp) ? (fp as Fp) : new Fp(fp as string | string[]);
    this.provider = rootFileSys;
    this.locks = new Set();
    this.active = true;
    this.endFns = [];
    
  }
  
  toString() { return `${getClsName(this)} @ ${this.fp.toString()}`; }
  
  checkFp(fp: Fp) {
    if (!this.fp.contains(fp))            throw Error('fp is not contained within the transaction')[mod]({ fp, tx: this });
    if (fp.cmps.some(cmp => cmp === '~')) throw Error('fp must not contain "~" component')[mod]({ fp });
  }
  locksCollide(lock0, lock1) {
    
    // Order `lock0` and `lock1` by their "type" properties
    if (lock0.type.localeCompare(lock1.type) > 0) [ lock0, lock1 ] = [ lock1, lock0 ];
    
    const collTypeKey = `${lock0.type}/${lock1.type}`;
    
    if (collTypeKey === 'nodeRead/nodeRead') return false; // Reads never collide with each other!
    
    if (collTypeKey === 'nodeRead/nodeWrite') {
      
      // Reads and writes only conflict if they occur on the exact same node
      return lock0.fp.equals(lock1.fp);
      
    }
    
    if (collTypeKey === 'nodeRead/subtreeWrite') {
      
      // Conflict if the node being read is within the subtree
      return lock1.fp.contains(lock0.fp);
      
    }
    
    if (collTypeKey === 'nodeWrite/nodeWrite') {
      
      // Writes aren't allowed to race with each other - two writes
      // collide if they occur on the exact same node!
      return lock0.fp.equals(lock1.fp);
      
    }
    
    if (collTypeKey === 'nodeWrite/subtreeWrite') {
      
      // Conflict if the node being written is within the subtree
      return lock1.fp.contains(lock0.fp);
      
    }
    
    if (collTypeKey === 'subtreeWrite/subtreeWrite') {
      
      // Conflict if either node contains the other; at first this intuitively feels like subtree
      // writes will almost always lock each other out, but this intuition is misleading! Tree-like
      // structures have "sufficient width" in such a way that, given two arbitrary nodes in any
      // large tree, it's unlikely either node contains the other. Consider two nodes "miss" each
      // other when their common ancestor is distinct from either of them (common!)
      return lock0.fp.contains(lock1.fp) || lock1.fp.contains(lock0.fp);
      
    }
    
    throw Error(`collision type "${collTypeKey}" not implemented`);
    
  }
  async doLocked<Fn extends () => any>({ name='?', locks=[], fn, err }: { name: string, locks: any[], fn: Fn, err?: any }): Promise<Awaited<ReturnType<Fn>>> {
    
    if (!this.active) throw Error('inactive transaction');
    
    for (const lock of locks) if (!lock[has]('prm')) lock.prm = Promise[later]();
    
    // Collect all pre-existing locks that collide with any of the locks
    // provided for this operation (once all collected Promises have
    // resolved we will be guaranteed we have a safely locked context!)
    const collLocks: any[] = [];
    for (const lk0 of this.locks) for (const lk1 of locks) if (this.locksCollide(lk0, lk1)) { collLocks.push(lk0); break; }
    
    // We've got our "prereq" Promise - now add a new Lock so any new
    // actions are blocked until `fn` completes
    for (const lock of locks) { this.locks.add(lock); lock.prm.then(() => this.locks[rem](lock)); }
    
    // Initialize the stack Error before any `await` gets called
    if (!err) err = Error('');
    
    // Wait for all collisions to resolve...
    await Promise.all(collLocks[map](lock => lock.prm)); // Won't reject because it's a Promise.all over Locks, and no `Lock(...).prm` ever rejects!
    
    // We now own the locked context!
    try          { return await fn(); }
    catch(cause) { throw err[mod]({ cause, msg: `Failed locked op: "${name}"` }); }
    finally      { for (const lock of locks) lock.prm.resolve(); } // Force any remaining Locks to resolve
    
  }
  async transact<T>({ name='?', fp, fn }: { name: string, fp: Fp, fn: (tx: Tx) => Promise<T> }) {
    
    // Maybe functions can pass in a whole bunch of initial locks with various bounding; the caller
    // can end these locks whenever they see fit (and `doLocked` can simply remove entries from
    // `this.locks` when the corresponding task resolves - not just at the end of the function!!)
    
    this.checkFp(fp);
    
    const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }) as LineageLock);
    return this.doLocked({ name: `tx/${name}`, locks: [ ...lineageLocks, { type: 'subtreeWrite', fp } ], fn: async () => {
      
      // Ensure all lineage Ents exist as Nodes, and resolve each lineage lock after the Node is
      // created
      // Consider that this is a bit early to be initiating the folder heirarchy, but currently a
      // bunch of operations use `this.fp.getLineage(trgFp)`; this implies that everything up until
      // `this.fp` already exists! If we weren't certain that a tx's root fp always existed, we
      // would have to do `filesystemRootFp.getLineage(trgFp)` instead. I think this change should
      // be made, as creating empty folders makes me sad. This change will require:
      // - `ensureLineageLocks` to expect a lineage always beginning from the system root
      // - `ensureLineageLocks` to have more efficient behaviour (e.g., check the top item for
      //   existence initially and if existing, immediately resolve all locks and short-circuit; or
      //   maybe just binary-search the lineage chain for the first non-existing node? in this case
      //   need to be careful to release the locks at the appropriate times)
      await this.provider.ensureLineageLocks(lineageLocks);
      
      const tx = new Tx(fp);
      try {
        const result = await fn(tx);
        await this.provider.remEmptyAncestors(fp.par());
        return result;
      } finally { tx.end(); }
      
    }});
    
  }
  async kid(fp: Fp | string | string[]) {
    
    // Returns `Promise<KidTransaction>`; example usage:
    //    | const kidTx = await rootTx.kid('C:/isolated');
    //    | // ... do a bunch of stuff with `kidTx` ...
    //    | kidTx.end();
    
    if (!isCls(fp, Fp)) fp = new Fp(fp);
    
    const kidPrm = Promise[later]<Tx>();
    this.transact({ name: 'kid', fp, fn: tx => {
      
      kidPrm.resolve(tx);
      
      const txDonePrm = Promise[later]();
      tx.endFns.push(() => txDonePrm.resolve());
      return txDonePrm;
      
    }});
    
    return kidPrm;
    
  }
  
  async getType(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getType', locks: [{ type: 'nodeRead', fp }], fn: () => this.provider.getType(fp) });
    
  }
  async getDataBytes(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getDataBytes', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      const stat = await this.provider.safeStat(fp);
      if (stat === null) return 0;
      if (stat.isFile()) return stat.size;
      
      // At this point `fp` is a directory; try to read the "~" file; any error results in 0 size
      const tildeStat = await this.provider.safeStat(fp.kid('~'));
      return tildeStat?.size ?? 0;
      
    }});
    
  }
  async setData(fp: Fp, data: null | string | Buffer) {
    
    this.checkFp(fp);
    
    if (!data?.length) {
      
      // Writing `null` implies a system-level delete (consider that reading a non-existing system
      // file resolves to `null`!)
      
      return this.doLocked({ name: 'setLeafEmpty', locks: [{ type: 'nodeWrite', fp }], fn: async () => {
        
        const type = await this.provider.getType(fp);
        if (type === null) return;
        
        const unlinkFp = {
          leaf: () => fp,        // For leafs simply unlink the leaf
          node: () => fp.kid('~') // For nodes try to unlink the "~" child
        }[type]();
        
        await this.provider.remNode(unlinkFp);
        await this.provider.remEmptyAncestors(unlinkFp!.par());
        
      }});
      
    } else {
      
      // Setting a non-zero amount of data requires ensuring that all
      // ancestor nodes exist and finally writing the data
      
      const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }));
      const nodeLock = { type: 'nodeWrite', fp };
      
      return this.doLocked({ name: 'setData', locks: [ ...lineageLocks, nodeLock ], fn: async () => {
        
        await this.provider.setData(lineageLocks, fp, data);
        
      }});
      
    }
    
  }
  async getData(fp: Fp, opts?: any) {
    
    this.checkFp(fp);
    
    return this.doLocked({ name: 'getData', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      return this.provider.getData(fp, opts);
      
    }});
    
  }
  getDataHeadStream(fp: Fp) {
    
    // A "head stream" ingresses into an fp's data. Writing to a fp with an active "head stream"
    // succeeds, but silently overwrites the content; for this reason conflicts need to be detected
    
    this.checkFp(fp);
    
    const streamPrm = Promise[later]<Writable>();
    
    const lineageLocks = [ ...this.fp.getLineage(fp) ][map](fp => ({ type: 'nodeWrite', fp, prm: Promise[later]() }));
    const nodeLock = { type: 'nodeWrite', fp };
    const prm = this.doLocked({ name: 'getHeadStream', locks: [ ...lineageLocks, nodeLock ], fn: async () => {
      
      // Ensure lineage
      const { stream, prm } = await this.provider.getDataSetterStreamAndFinalizePrm(lineageLocks, fp);
      
      // Expose the stream immediately
      streamPrm.resolve(stream);
      
      // Don't allow `doLocked` to finish until the stream is finalized (need to maintain locks)
      await prm;
      
    }});
    
    // Expose the head stream, with a "prm" attribute attached which can allow the consumer to
    // await operation completion
    return streamPrm.then(stream => Object.assign(stream, { prm }));
    
  }
  getDataTailStream(fp: Fp) {
    
    // A "tail stream" egresses from a file pointer's data storage. Once a stream has initialized,
    // it seems unaffected even if the file pointer is changed partway through - this means we can
    // release locks (allow the `doLocked` async fn to resolve) immediately!
    
    this.checkFp(fp);
    
    const streamPrm = Promise[later]<Readable>();
    
    const nodeLock = { type: 'nodeRead', fp };
    const prm = this.doLocked({ name: 'getTailStream', locks: [ nodeLock ], fn: async () => {
      
      const { stream, prm } = await this.provider.getDataGetterStreamAndFinalizePrm(fp);
      streamPrm.resolve(stream); // Pass the initialized stream to the caller
      await prm;
      
    }});
    
    return streamPrm.then(stream => Object.assign(stream, { prm }));
    
  }
  
  async getKidNames(fp: Fp) {
    
    this.checkFp(fp);
    return this.doLocked({ name: 'getKidNames', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      return this.provider.getKidCmps(fp);
      
    }});
    
  }
  async remSubtree(fp: Fp) {
    
    this.checkFp(fp);
    
    return this.doLocked({ name: 'remSubtree', locks: [{ type: 'subtreeWrite', fp }], err: Error(''), fn: async () => {
      return this.provider.remSubtree(fp);
    }});
    
  }
  async iterateNode(fp: Fp, { bufferSize=150 }={}) {
    
    this.checkFp(fp);
    
    const itPrm = Promise[later]<PartialIterator<string>>();
    
    const prm = this.doLocked({ name: 'iterateNode', locks: [{ type: 'nodeRead', fp }], fn: async () => {
      
      const iterator = await this.provider.getKidIteratorAndFinalizePrm(fp, { bufferSize });
      itPrm.resolve(iterator);
      await iterator.prm;
      
    }});
    
    const tx = this;
    return itPrm.then(it => ({
      
      async* [Symbol.asyncIterator]() {
        
        for await (const fd of it)
          yield new Ent(fp.kid(fd), { tx });
        
      },
      close() { return it.close(); },
      prm
      
    }));
    
  }
  
  end() {
    
    // TODO: what happens when this is called on a Tx with some KidTxs?? We can't safely end the
    // Par until all the Kids are ended...
    return this.doLocked({ name: 'deactivate', locks: [], fn: async () => {
      this.active = true;
      for (const fn of this.endFns) fn();
    }});
    
  }
  
};

type EncodeTerm = null | 'utf8' | 'base64' | 'json';
export class Ent {
  
  // I think the rightful philosophy is to deny traversal from Kid -> Par; to access a shallower
  // Ent, need access to some Ent shallow enough to contain that Ent - note a `par` function
  // exists but doesn't traverse any shallower than the Tx backing the Ent (note consumers could
  // already use `new Ent(ent.tx.fp).kid(...)` to access anything within the transaction)
  
  public fp: Fp;
  public tx: Tx;
  
  constructor(fp: string | string[] | Fp, { tx = rootTx }={}) {
    
    if (isCls(fp, String) || isCls(fp, Array)) fp = new Fp(fp);
    for (const cmp of fp.cmps) if (/^[~]+$/.test(cmp)) throw Error('Illegal cmp must include char other than "~"')[mod]({ fp, cmp });
    
    this.fp = fp;
    this.tx = tx;
    
  }
  
  getCmps() { return this.fp.cmps; }
  
  kid(relFp: string | string[]): Ent;
  kid(relFp: string | string[], opts: {}): Ent;
  kid(relFp: string | string[], opts: { newTx: false }): Ent;
  kid(relFp: string | string[], opts: { newTx: true }): Promise<Ent>;
  kid(relFp: string | string[], opts?: { newTx?: boolean }): Promise<Ent> | Ent {
    
    const kidFp = this.fp.kid(relFp);
    if (opts?.newTx) {
      
      return this.tx.kid(kidFp).then(tx => new Ent(kidFp, { tx })) as Promise<Ent> as any;
      
    } else {
      
      return new Ent(kidFp, { tx: this.tx });
      
    }
    
  }
  par(): Ent {
    
    if (this.tx.fp.equals(this.fp)) throw Error('parent is outside transaction');
    
    return new Ent(this.fp.par(), { tx: this.tx });
    
  }
  
  // Data
  async getData():               Promise<Buffer>;
  async getData(opts: null):     Promise<Buffer>;
  async getData(opts: 'utf8'):   Promise<string>;
  async getData(opts: 'base64'): Promise<string>;
  async getData(opts: 'json'):   Promise<Json>;
  async getData(opts?: EncodeTerm | { encoding: EncodeTerm }) {
    
    const content = await this.tx.getData(this.fp, opts === 'json' ? null : opts);
    if (!content.length) {
      if (opts === null)     return Buffer.alloc(0);
      if (opts === 'utf8')   return '';
      if (opts === 'base64') return '';
      if (opts === 'json')   return null;
    }
    
    if (opts === 'json') return JSON.parse(content);
    
    return content;
    
  }
  
  // When opts are omitted, Buffer becomes an option, and strings remain in utf8
  async setData(data: null | string | Buffer, opts?: null);
  async setData(data: null | string,          opts: 'utf8');
  async setData(data: null | string,          opts: 'base64');
  async setData(data: Json,                   opts: 'json');
  async setData(data, opts: any = null) {
    
    const dat = (() => {
      
      if (opts === null)   return data;
      if (opts === 'json') return JSON.stringify(data);
      return Buffer.from(data as string, opts);
      
    })();
    
    return this.tx.setData(this.fp, dat);
    
  }
  async getDataBytes() { return this.tx.getDataBytes(this.fp); }
  async exists() { return this.getDataBytes().then(v => v > 0); }
  async rem() { return this.tx.remSubtree(this.fp); }
  async getDataHeadStream() { return this.tx.getDataHeadStream(this.fp); }
  async getDataTailStream() { return this.tx.getDataTailStream(this.fp); }
  async getKids(): Promise<Obj<Ent>> {
    const names = await this.tx.getKidNames(this.fp);
    return names[toObj](name => [ name, this.kid([ name ]) ]);
  }
  kids() {
    return this.tx.iterateNode(this.fp);
  }
  toString() { return this.fp.toString(); }
  
};

export const rootFileSys = new FileSys();
export const rootTx = new Tx([]) as any as Tx;
export const rootEnt = new Ent(rootTx.fp, { tx: rootTx });
