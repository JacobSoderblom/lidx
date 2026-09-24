class SecretHelper {
  // Private: only SecretHelper's own members may call it directly. #75
  // fixture (paired with visibilityPrivateProber.ts): a cross-file
  // two-segment fallback call to this must stay UNRESOLVED.
  private static doSecret(): string {
    return "secret";
  }

  static callLocally(): string {
    return SecretHelper.doSecret();
  }
}
