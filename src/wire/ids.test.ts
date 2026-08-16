import { describe, it, expect } from 'vitest';
import { createIdAllocator } from './ids';

describe('createIdAllocator', () => {
  it('starts at 1 by default and increments monotonically', () => {
    const a = createIdAllocator();
    expect(a.next()).toBe(1);
    expect(a.next()).toBe(2);
    expect(a.next()).toBe(3);
    expect(a.floor).toBe(3);
  });

  it('respects the initial value', () => {
    const a = createIdAllocator(100);
    expect(a.next()).toBe(101);
    expect(a.next()).toBe(102);
  });

  it('setFloor raises but never lowers', () => {
    const a = createIdAllocator(5);
    a.setFloor(50);
    expect(a.next()).toBe(51);
    a.setFloor(10); // lower — no-op
    expect(a.next()).toBe(52);
  });

  it('multiple allocators are independent', () => {
    const a = createIdAllocator();
    const b = createIdAllocator();
    a.next(); a.next();
    expect(b.next()).toBe(1);
    expect(a.floor).toBe(2);
    expect(b.floor).toBe(1);
  });
});
