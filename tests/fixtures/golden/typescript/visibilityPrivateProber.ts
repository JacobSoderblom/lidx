class Prober {
  // Calls SecretHelper.doSecret via an unimported class reference (no
  // `import { SecretHelper }`), so there's no import candidate and this
  // reaches the two-segment name fallback. Before #75, `SecretHelper.
  // doSecret` was the sole two-segment match in the whole fixture and
  // bound despite being private and declared in a different file. #75's
  // visibility guard must refuse it.
  callRemotely(): string {
    return SecretHelper.doSecret();
  }
}
