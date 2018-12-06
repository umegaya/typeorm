import { PlatformTools } from '../../platform/PlatformTools';


let globalScope = PlatformTools.getGlobalVariable();
let getRandomBytes = (
    (typeof globalScope !== 'undefined' && (globalScope.crypto || globalScope.msCrypto))
    ? function() { // Browsers
        var crypto = (globalScope.crypto || globalScope.msCrypto), QUOTA = 65536;
        return function(n: number) {
            var a = new Uint8Array(n);
            for (var i = 0; i < n; i += QUOTA) {
            crypto.getRandomValues(a.subarray(i, i + Math.min(n - i, QUOTA)));
            }
            return a;
        };
    }
    : function() { // Node
        return require("crypto").randomBytes;
    }
)();

export class SpannerUtil {
    static randomBytes(size: number): Uint8Array {
        return getRandomBytes(size);
    }
}
