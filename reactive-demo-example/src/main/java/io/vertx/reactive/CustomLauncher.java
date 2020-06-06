package io.vertx.reactive;

import io.vertx.core.Launcher;

public class CustomLauncher extends Launcher {
  @Override
  protected String getMainVerticle() {
    return ServerVerticle.class.getName();
  }
  public static void main(String[] args) {
    new Launcher().dispatch(args);
  }
}