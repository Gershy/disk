import nodeFs from 'node:fs';
import { inCls, safe } from '@gershy/clearing';

export default (() => {
  
  type FsPrm = (typeof nodeFs)['promises'] & Pick<typeof nodeFs, 'createReadStream' | 'createWriteStream'>;
  return ({ ...nodeFs.promises, ...nodeFs[slice]([ 'createReadStream', 'createWriteStream' ]) } as FsPrm)
  
    [map]((fsVal: any, name): any => {
      
      // Include any non-function members of node:fs as-is
      if (!inCls(fsVal, Function)) return fsVal;
      
      // Functions become wrapped for better error reporting (especially stacktrace)
      return (...args) => {
        
        const err = Error();
        return safe(
          () => fsVal(...args),
          cause => err[fire]({ cause, msg: `Failed low-level ${name} on "${args[0]}"` })
        );
        
      };
      
    }) as FsPrm;
  
})();