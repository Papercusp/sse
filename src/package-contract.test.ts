import { readFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { beforeAll, describe, expect, it } from 'vitest';

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
  devDependencies: Record<string, string>;
  exports: Record<string, ExportTarget>;
};

type TypeScriptConfig = {
  compilerOptions: {
    module: string;
    moduleResolution: string;
    ignoreDeprecations?: string;
  };
};

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const manifest = JSON.parse(
  readFileSync(resolve(packageRoot, 'package.json'), 'utf8'),
) as PackageManifest;
const tsconfig = JSON.parse(
  readFileSync(resolve(packageRoot, 'tsconfig.json'), 'utf8'),
) as TypeScriptConfig;

beforeAll(() => {
  execFileSync(process.platform === 'win32' ? 'npm.cmd' : 'npm', ['run', 'build'], {
    cwd: packageRoot,
    stdio: 'pipe',
  });
});

describe('@papercusp/sse package contract', () => {
  it('uses the package-owned TypeScript 6 compiler boundary', () => {
    const requireFromPackage = createRequire(resolve(packageRoot, 'package.json'));
    const compilerManifest = JSON.parse(
      readFileSync(requireFromPackage.resolve('typescript/package.json'), 'utf8'),
    ) as { version: string };

    expect(manifest.devDependencies.typescript).toBe('~6.0.0');
    expect(compilerManifest.version).toMatch(/^6\.0\./);
    expect(tsconfig.compilerOptions).toMatchObject({
      module: 'CommonJS',
      moduleResolution: 'Bundler',
    });
    expect(tsconfig.compilerOptions.ignoreDeprecations).toBeUndefined();
  });

  it('routes ESM imports to source and Node requires to built CommonJS', () => {
    expect(manifest.type).toBe('module');
    expect(manifest.main).toBe('dist/index.js');
    expect(manifest.scripts.build).toContain('tsc');
    expect(manifest.scripts.build).toContain('dist/index.js');
    expect(manifest.scripts.build).not.toContain('&&');
    expect(manifest.scripts.typecheck).toBe('tsc -p tsconfig.json --noEmit');

    expect(manifest.exports).toEqual({
      '.': {
        types: './src/index.ts',
        import: './src/index.ts',
        require: './dist/index.js',
        default: './dist/index.js',
      },
      './react': {
        types: './src/react.ts',
        import: './src/react.ts',
        require: './dist/react.js',
        default: './dist/react.js',
      },
      './postgres': {
        types: './src/postgres.ts',
        import: './src/postgres.ts',
        require: './dist/postgres.js',
        default: './dist/postgres.js',
      },
    });
  });

  it('loads the built main entry through the exact CommonJS package specifier', () => {
    const distManifest = JSON.parse(
      readFileSync(resolve(packageRoot, 'dist/package.json'), 'utf8'),
    ) as { type: string };
    const requireFromPackage = createRequire(resolve(packageRoot, 'package.json'));
    const loaded = requireFromPackage('@papercusp/sse') as Record<string, unknown>;

    expect(distManifest.type).toBe('commonjs');
    expect(loaded.sseResponse).toBeTypeOf('function');
    expect(loaded.sseResponseToNode).toBeTypeOf('function');
    expect(loaded.getChannel).toBeTypeOf('function');
  });

  it('links named source exports through the exact ESM package specifier', () => {
    const output = execFileSync(
      process.execPath,
      [
        '--import',
        'tsx',
        '--input-type=module',
        '--eval',
        "import { getChannel } from '@papercusp/sse'; process.stdout.write(typeof getChannel);",
      ],
      { cwd: packageRoot, encoding: 'utf8' },
    );

    expect(output).toBe('function');
  });
});
