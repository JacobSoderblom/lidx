export class Greeter {
  // Method call on `this`.
  greet(name: string): string {
    return this.format(name);
  }

  private format(name: string): string {
    return "Hello";
  }
}

// Decoy: a second, unrelated `greet` and `format`.
export class LoudGreeter {
  greet(name: string): string {
    return "HELLO";
  }

  private format(name: string): string {
    return "LOUD";
  }
}
