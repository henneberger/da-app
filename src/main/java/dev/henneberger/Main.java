package dev.henneberger;

import dev.henneberger.operator.OperatorRunner;
import dev.henneberger.runtime.RuntimeMain;

public final class Main {

  private Main() {
  }

  public static void main(String[] args) {
    String mode = System.getenv().getOrDefault("VERTX_MODE", "operator");
    if ("runtime".equalsIgnoreCase(mode)) {
      RuntimeMain.start();
    } else {
      OperatorRunner.start();
    }
  }
}
