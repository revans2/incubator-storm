namespace java org.apache.storm.generated

exception HBAuthorizationException {
  1: required string msg;
}

exception HBExecutionException {
  1: required string msg;
}

struct Pulse {
  1: required string id;
  2: binary details;
}

struct HBRecords {
  1: list<Pulse> pulses;
}

struct HBNodes {
  1: list<string> pulseIds;
}

service HBServer {
  void createPath(1: string path) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  bool exists(1: string path) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  void sendPulse(1: Pulse pulse) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  HBRecords getAllPulseForPath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  HBNodes getAllNodesForPath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  Pulse getPulse(1: string id) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  void deletePath(1: string idPrefix) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
  void deletePulseId(1: string id) throws (1: HBExecutionException e, 2: HBAuthorizationException aze);
}