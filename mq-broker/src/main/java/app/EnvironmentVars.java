package app;

public enum EnvironmentVars {
  BROKER_ID("BROKER_ID"),
  BROKER_HOSTNAME("BROKER_HOSTNAME"),
  PEER_HOSTNAMES("PEER_HOSTNAMES");

  private final String name;

  EnvironmentVars(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
