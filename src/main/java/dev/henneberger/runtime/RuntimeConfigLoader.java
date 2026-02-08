package dev.henneberger.runtime;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.henneberger.runtime.config.RuntimeConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class RuntimeConfigLoader {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private RuntimeConfigLoader() {
  }

  public static RuntimeConfig load(Path path) {
    try {
      byte[] payload = Files.readAllBytes(path);
      return MAPPER.readValue(payload, RuntimeConfig.class);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load runtime config from " + path, e);
    }
  }
}
