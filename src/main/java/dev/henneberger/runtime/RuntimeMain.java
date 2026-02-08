package dev.henneberger.runtime;

import dev.henneberger.runtime.config.RuntimeConfig;
import java.nio.file.Path;

public final class RuntimeMain {

  private RuntimeMain() {
  }

  public static void start() {
    String configPath = System.getenv().getOrDefault("CONFIG_PATH", "/config/config.json");
    RuntimeConfig config = RuntimeConfigLoader.load(Path.of(configPath));
    GraphQLServer.start(config);
  }
}
