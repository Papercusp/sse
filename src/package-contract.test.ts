import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { describe, expect, it } from 'vitest';

type ExportTarget = {
  types: string;
  import: string;
  require: string;
  default: string;
};

type PackageManifest = {
  type: string;
  main: string;
  scripts: Record<string, string>;
  exports: Record<string, ExportTarget>;
};

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(
  readFileSync(resolve(packageRoot, 'package.json'), 'utf8'),
) as PackageManifest;

describe('@papercusp/sse package contract', () => {
  it('routes web imports to source and Node requires to built CommonJS', () => {
    expect(manifest.type).toBe('commonjs');
    expect(manifest.main).toBe('dist/index.js');
    expect(manifest.scripts.build).toBe('tsc -p tsconfig.json');
    expect(manifest.scripts.typecheck).toBe('tsc -p tsconfig.json --noEmit');

    expect(manifest.exports).toEqual({
      '.': {
        types: './src/index.ts',
        import: './src/index.ts',
        require: './dist/index.js',
        default: './src/index.ts',
      },
      './react': {
        types: './src/react.ts',
        import: './src/react.ts',
        require: './dist/react.js',
        default: './src/react.ts',
      },
      './postgres': {
        types: './src/postgres.ts',
        import: './src/postgres.ts',
        require: './dist/postgres.js',
        default: './src/postgres.ts',
      },
    });
  });

  it('loads the built main entry through the exact CommonJS package specifier', () => {
    const requireFromPackage = createRequire(resolve(packageRoot, 'package.json'));
    const loaded = requireFromPackage('@papercusp/sse') as Record<string, unknown>;

    expect(loaded.sseResponse).toBeTypeOf('function');
    expect(loaded.sseResponseToNode).toBeTypeOf('function');
    expect(loaded.getChannel).toBeTypeOf('function');
  });
});
