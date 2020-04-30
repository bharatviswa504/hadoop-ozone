package org.apache.hadoop.protobuf;

import com.google.protobuf.ByteString;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class which has methods related to Security token stuff.
 */
public final class ProtoHelper {

  private ProtoHelper() {

  }

  /**
   * Map used to cache fixed strings to ByteStrings. Since there is no
   * automatic expiration policy, only use this for strings from a fixed, small
   * set.
   * <p/>
   * This map should not be accessed directly. Used the getFixedByteString
   * methods instead.
   */
  private final static ConcurrentHashMap<Object, ByteString>
      FIXED_BYTESTRING_CACHE = new ConcurrentHashMap<>();


  public static TokenProto protoFromToken(Token<?> tok) {
    TokenProto.Builder builder = TokenProto.newBuilder().
        setIdentifier(getByteString(tok.getIdentifier())).
        setPassword(getByteString(tok.getPassword())).
        setKindBytes(getFixedByteString(tok.getKind().toString())).
        setServiceBytes(getFixedByteString(tok.getService().toString()));
    return builder.build();
  }

  public static Token<? extends TokenIdentifier > tokenFromProto(
      TokenProto tokenProto) {
    Token<? extends TokenIdentifier> token = new Token<>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(), new Text(tokenProto.getKind()),
        new Text(tokenProto.getService()));
    return token;
  }

  public static ByteString getByteString(byte[] bytes) {
    // return singleton to reduce object allocation
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  /**
   * Get the ByteString for frequently used fixed and small set strings.
   * @param key string
   * @return
   */
  public static ByteString getFixedByteString(String key) {
    ByteString value = FIXED_BYTESTRING_CACHE.get(key);
    if (value == null) {
      value = ByteString.copyFromUtf8(key);
      FIXED_BYTESTRING_CACHE.put(key, value);
    }
    return value;
  }


}
