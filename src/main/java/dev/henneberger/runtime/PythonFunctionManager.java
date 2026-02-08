package dev.henneberger.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.henneberger.runtime.config.PythonFunctionConfig;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes Python functions defined via PythonFunction custom resources.
 *
 * Implementation notes:
 * - We create a per-function virtualenv in /tmp and install inline requirements.txt content.
 * - We keep a pool of long-lived python worker processes per function (per-function venv).
 */
public final class PythonFunctionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PythonFunctionManager.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String MODULE_NAME = "udf";

  private final Map<String, PythonFunctionConfig> index;
  private final Map<String, Object> buildLocks = new ConcurrentHashMap<>();
  private final Map<String, FunctionPool> pools = new ConcurrentHashMap<>();
  private final int defaultWorkers;

  public PythonFunctionManager(List<PythonFunctionConfig> functions) {
    Map<String, PythonFunctionConfig> idx = new HashMap<>();
    if (functions != null) {
      for (PythonFunctionConfig fn : functions) {
        if (fn == null || fn.getName() == null || fn.getName().isBlank()) {
          continue;
        }
        idx.put(fn.getName(), fn);
      }
    }
    this.index = Collections.unmodifiableMap(idx);
    this.defaultWorkers = parseIntEnv("PYTHON_FUNCTION_WORKERS", 2);
  }

  public boolean isEnabled() {
    return !index.isEmpty();
  }

  public PythonFunctionConfig get(String name) {
    return index.get(name);
  }

  public Object invokeBlocking(String functionRef,
                               List<Object> args,
                               Integer timeoutMsOverride) {
    Objects.requireNonNull(functionRef, "functionRef is required");
    PythonFunctionConfig fn = index.get(functionRef);
    if (fn == null) {
      throw new IllegalStateException("PythonFunction " + functionRef + " not configured");
    }
    if (fn.getFunction() == null || fn.getFunction().getName() == null || fn.getFunction().getName().isBlank()) {
      throw new IllegalStateException("PythonFunction " + functionRef + " missing function.name");
    }
    if (fn.getFunction().getModule() == null
      || fn.getFunction().getModule().getInline() == null
      || fn.getFunction().getModule().getInline().isBlank()) {
      throw new IllegalStateException("PythonFunction " + functionRef + " missing function.module.inline");
    }

    int timeoutMs = effectiveTimeoutMs(fn, timeoutMsOverride);
    FunctionPool pool = pools.computeIfAbsent(functionRef, ignored -> buildPool(functionRef, fn));
    Worker worker = null;
    try {
      worker = pool.idle.take();
      try {
        return worker.call(args == null ? List.of() : args, timeoutMs);
      } catch (Exception e) {
        // If the worker got wedged/crashed, replace it so the pool remains healthy.
        safeDestroy(worker);
        Worker replacement = startWorker(functionRef, fn, pool.dir);
        pool.idle.offer(replacement);
        worker = null;
        throw e;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("PythonFunction " + functionRef + " invoke interrupted", e);
    } finally {
      if (worker != null) {
        pool.idle.offer(worker);
      }
    }
  }

  private void safeDestroy(Worker worker) {
    if (worker == null) {
      return;
    }
    try {
      worker.process.destroy();
    } catch (Exception ignored) {
    }
    try {
      worker.process.destroyForcibly();
    } catch (Exception ignored) {
    }
  }

  public CompletableFuture<Object> invokeAsync(io.vertx.core.Vertx vertx,
                                               String functionRef,
                                               List<Object> args,
                                               Integer timeoutMsOverride) {
    CompletableFuture<Object> future = new CompletableFuture<>();
    vertx.executeBlocking(promise -> {
      try {
        Object result = invokeBlocking(functionRef, args, timeoutMsOverride);
        promise.complete(result);
      } catch (Exception e) {
        promise.fail(e);
      }
    }, ar -> {
      if (ar.failed()) {
        future.completeExceptionally(ar.cause());
      } else {
        future.complete(ar.result());
      }
    });
    return future;
  }

  private int effectiveTimeoutMs(PythonFunctionConfig fn, Integer override) {
    if (override != null && override > 0) {
      return override;
    }
    if (fn.getTimeoutMs() != null && fn.getTimeoutMs() > 0) {
      return fn.getTimeoutMs();
    }
    return 30_000;
  }

  private void ensurePrepared(PythonFunctionConfig fn, Path dir) {
    String lockKey = dir.toString();
    Object lock = buildLocks.computeIfAbsent(lockKey, k -> new Object());
    synchronized (lock) {
      try {
        Path ready = dir.resolve(".ready");
        if (Files.exists(ready)) {
          return;
        }
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(MODULE_NAME + ".py"), fn.getFunction().getModule().getInline(), StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("worker.py"), workerSource(), StandardCharsets.UTF_8);

        if (fn.getRequirements() != null
          && fn.getRequirements().getInline() != null
          && !fn.getRequirements().getInline().isBlank()) {
          Files.writeString(dir.resolve("requirements.txt"), fn.getRequirements().getInline(), StandardCharsets.UTF_8);
        }

        run(dir, List.of("python3", "-m", "venv", "venv"), 120_000);
        Path requirements = dir.resolve("requirements.txt");
        if (Files.exists(requirements)) {
          run(dir, List.of(dir.resolve("venv").resolve("bin").resolve("pip").toString(), "install", "-r", "requirements.txt"), 600_000);
        }

        Files.writeString(ready, "ok\n", StandardCharsets.UTF_8);
      } catch (Exception e) {
        throw new IllegalStateException("Failed preparing PythonFunction venv at " + dir, e);
      }
    }
  }

  private void run(Path dir, List<String> cmd, long timeoutMs) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.directory(dir.toFile());
    pb.redirectErrorStream(true);
    Process p = pb.start();
    boolean finished = p.waitFor(timeoutMs, TimeUnit.MILLISECONDS);
    byte[] out = p.getInputStream().readAllBytes();
    if (!finished) {
      p.destroyForcibly();
      throw new IllegalStateException("Command timed out: " + String.join(" ", cmd));
    }
    int code = p.exitValue();
    if (code != 0) {
      String text = new String(out, StandardCharsets.UTF_8);
      throw new IllegalStateException("Command failed exitCode=" + code + " cmd=" + String.join(" ", cmd) + " output=" + text);
    }
    LOGGER.info("Python prep ok cmd={} dir={}", String.join(" ", cmd), dir);
  }

  private FunctionPool buildPool(String functionRef, PythonFunctionConfig fn) {
    String key = buildKey(fn);
    Path dir = Path.of("/tmp", "vertx-python-functions", functionRef + "-" + key);
    ensurePrepared(fn, dir);

    int workers = defaultWorkers;
    if (workers <= 0) {
      workers = 1;
    }
    BlockingQueue<Worker> idle = new ArrayBlockingQueue<>(workers);
    for (int i = 0; i < workers; i++) {
      idle.add(startWorker(functionRef, fn, dir));
    }
    LOGGER.info("PythonFunction pool ready functionRef={} workers={} dir={}", functionRef, workers, dir);
    return new FunctionPool(dir, idle);
  }

  private Worker startWorker(String functionRef, PythonFunctionConfig fn, Path dir) {
    String python = dir.resolve("venv").resolve("bin").resolve("python").toString();
    Path worker = dir.resolve("worker.py");

    ProcessBuilder pb = new ProcessBuilder(
      python,
      worker.toString(),
      MODULE_NAME,
      fn.getFunction().getName()
    );
    pb.directory(dir.toFile());
    pb.redirectErrorStream(false); // keep stdout clean for JSON protocol; logs go to stderr
    pb.environment().putIfAbsent("PYTHONUTF8", "1");

    try {
      Process p = pb.start();
      BufferedWriter stdin = new BufferedWriter(new OutputStreamWriter(p.getOutputStream(), StandardCharsets.UTF_8));
      BufferedReader stdout = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
      BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8));
      startStderrLogger(functionRef, stderr);
      return new Worker(functionRef, dir, p, stdin, stdout);
    } catch (IOException e) {
      throw new IllegalStateException("Failed starting Python worker for " + functionRef, e);
    }
  }

  private void startStderrLogger(String functionRef, BufferedReader stderr) {
    Thread t = new Thread(() -> {
      try {
        String line;
        while ((line = stderr.readLine()) != null) {
          if (!line.isBlank()) {
            LOGGER.info("python[{}] {}", functionRef, line);
          }
        }
      } catch (Exception e) {
        // ignore
      }
    }, "python-fn-" + functionRef + "-stderr");
    t.setDaemon(true);
    t.start();
  }

  private String buildKey(PythonFunctionConfig fn) {
    String req = fn.getRequirements() == null ? "" : Objects.toString(fn.getRequirements().getInline(), "");
    String mod = fn.getFunction() == null || fn.getFunction().getModule() == null ? "" : Objects.toString(fn.getFunction().getModule().getInline(), "");
    String name = fn.getFunction() == null ? "" : Objects.toString(fn.getFunction().getName(), "");
    return sha256Base64Url(req + "\n---\n" + name + "\n---\n" + mod);
  }

  private static String sha256Base64Url(String text) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(text.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    } catch (Exception e) {
      throw new IllegalStateException("sha256 failed", e);
    }
  }

  private static int parseIntEnv(String key, int defaultValue) {
    try {
      String raw = System.getenv(key);
      if (raw == null || raw.isBlank()) {
        return defaultValue;
      }
      return Integer.parseInt(raw.trim());
    } catch (Exception e) {
      return defaultValue;
    }
  }

  private static String workerSource() {
    return String.join("\n",
      "import asyncio",
      "import contextlib",
      "import importlib",
      "import inspect",
      "import json",
      "import signal",
      "import sys",
      "import traceback",
      "",
      "def _timeout_handler(signum, frame):",
      "  raise TimeoutError('python function timeout')",
      "",
      "def _call(fn, args, timeout_ms):",
      "  if timeout_ms and timeout_ms > 0:",
      "    try:",
      "      signal.signal(signal.SIGALRM, _timeout_handler)",
      "      signal.setitimer(signal.ITIMER_REAL, timeout_ms / 1000.0)",
      "    except Exception:",
      "      pass",
      "  try:",
      "    # Keep stdout clean for the JSON protocol; send user prints to stderr.",
      "    with contextlib.redirect_stdout(sys.stderr):",
      "      res = fn(*args)",
      "      if inspect.isawaitable(res):",
      "        res = asyncio.run(res)",
      "      return res",
      "  finally:",
      "    try:",
      "      signal.setitimer(signal.ITIMER_REAL, 0)",
      "    except Exception:",
      "      pass",
      "",
      "def main():",
      "  mod_name = sys.argv[1]",
      "  fn_name = sys.argv[2]",
      "  mod = importlib.import_module(mod_name)",
      "  fn = getattr(mod, fn_name)",
      "  for line in sys.stdin:",
      "    line = line.strip()",
      "    if not line:",
      "      continue",
      "    try:",
      "      payload = json.loads(line)",
      "      args = payload.get('args') or []",
      "      timeout_ms = payload.get('timeoutMs')",
      "      res = _call(fn, args, timeout_ms)",
      "      sys.stdout.write(json.dumps({'ok': True, 'result': res}) + \"\\n\")",
      "      sys.stdout.flush()",
      "    except Exception as e:",
      "      sys.stdout.write(json.dumps({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}) + \"\\n\")",
      "      sys.stdout.flush()",
      "",
      "if __name__ == '__main__':",
      "  main()",
      ""
    );
  }

  private static final class FunctionPool {
    private final Path dir;
    private final BlockingQueue<Worker> idle;

    private FunctionPool(Path dir, BlockingQueue<Worker> idle) {
      this.dir = dir;
      this.idle = idle;
    }
  }

  private static final class Worker {
    private final String functionRef;
    private final Path dir;
    private final Process process;
    private final BufferedWriter stdin;
    private final BufferedReader stdout;

    private Worker(String functionRef, Path dir, Process process, BufferedWriter stdin, BufferedReader stdout) {
      this.functionRef = functionRef;
      this.dir = dir;
      this.process = process;
      this.stdin = stdin;
      this.stdout = stdout;
    }

    private Object call(List<Object> args, int timeoutMs) {
      if (!process.isAlive()) {
        throw new IllegalStateException("Python worker process is not alive for " + functionRef);
      }
      Map<String, Object> payload = new HashMap<>();
      payload.put("args", args);
      payload.put("timeoutMs", timeoutMs);
      try {
        stdin.write(MAPPER.writeValueAsString(payload));
        stdin.write("\n");
        stdin.flush();
        String line = stdout.readLine();
        if (line == null) {
          throw new IllegalStateException("Python worker closed stdout for " + functionRef);
        }
        Map<?, ?> result = MAPPER.readValue(line, Map.class);
        Object ok = result.get("ok");
        if (!(ok instanceof Boolean) || !((Boolean) ok)) {
          Object err = result.get("error");
          Object tb = result.get("traceback");
          throw new IllegalStateException("PythonFunction " + functionRef + " error=" + err + " traceback=" + tb);
        }
        return result.get("result");
      } catch (Exception e) {
        throw new IllegalStateException("PythonFunction " + functionRef + " invoke failed", e);
      }
    }
  }
}
