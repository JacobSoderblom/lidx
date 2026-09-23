export interface Speaker {
  say(): string;
}

export class Animal {
  // Base method `Dog` inherits without overriding.
  speak(): string {
    return "...";
  }
}

export class Dog extends Animal {
  bark(): string {
    return "Woof";
  }
}

// Decoy: an unrelated class with its own `speak` and `say`.
export class Cat implements Speaker {
  speak(): string {
    return "Meow";
  }

  say(): string {
    return "Meow";
  }
}
