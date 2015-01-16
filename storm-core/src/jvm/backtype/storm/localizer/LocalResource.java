package backtype.storm.localizer;

/**
 * Local Resource requested by the topology
 */
public class LocalResource {
  private String _blobKey;
  private boolean _uncompress;

  public LocalResource(String keyname, boolean uncompress) {
    _blobKey = keyname;
    _uncompress = uncompress;
  }

  public String getBlobName() {
    return _blobKey;
  }

  public boolean shouldUncompress() {
    return _uncompress;
  }

  @Override
  public String toString() {
    return "Key: " + _blobKey + " uncompress: " + _uncompress;
  }
}
