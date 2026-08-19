import { sharedHostWorkerCap } from '@papercusp/test-config/vitest-config';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    // jsdom for the React hook test; per-file pragmas would do too, but
    // simpler to use a project-level split if we grow more browser tests.
    environmentMatchGlobs: [
      ['src/client/use-event-source.test.ts', 'jsdom'],
      ['src/__tests__/use-event-source.test.ts', 'jsdom'],
      ['src/client/resilient-event-source.visibility.test.ts', 'jsdom'],
    ],
    // Integration tests that need real PG are tagged with `.pg.test.ts`
    // and excluded from the default run. Run them via `npx vitest run --include 'src/**/*.pg.test.ts'`.
    exclude: ['node_modules', 'dist', '**/*.pg.test.ts'],
    testTimeout: 15_000,
    // See libs/sync/vitest.config.ts — every project in the root topology must
    // agree on maxWorkers or vitest 4 refuses the run.
    ...sharedHostWorkerCap(),
  },
});
