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
  it('uses native TypeScript 7 with the official TypeScript 6 API compatibility boundary', () => {
    const requireFromPackage = createRequire(resolve(packageRoot, 'package.json'));
    const compilerManifest = JSON.parse(
      readFileSync(requireFromPackage.resolve('typescript/package.json'), 'utf8'),
    ) as { name: string; version: string; bin: Record<string, string> };
    const nativeManifest = JSON.parse(
      readFileSync(requireFromPackage.resolve('@typescript/native/package.json'), 'utf8'),
    ) as { name: string; version: string; bin: Record<string, string> };

    expect(manifest.devDependencies['@typescript/native']).toBe('npm:typescript@7.0.2');
    expect(manifest.devDependencies.typescript).toBe(
      'npm:@typescript/typescript6@6.0.2',
    );
    expect(nativeManifest).toMatchObject({
      name: 'typescript',
      version: '7.0.2',
      bin: { tsc: './bin/tsc' },
    });
    expect(compilerManifest).toMatchObject({
      name: '@typescript/typescript6',
      version: '6.0.2',
      bin: { tsc6: './bin/tsc6' },
    });
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
    for (const script of [manifest.scripts.build, manifest.scripts.typecheck]) {
      expect(script).toContain("require.resolve('@typescript/native/package.json')");
    }
    expect(manifest.scripts.typecheck).toContain("'-p', 'tsconfig.json', '--noEmit'");

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
