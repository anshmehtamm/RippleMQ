package app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ApplicationMainTest {

  @Test
  void exitsWhenBrokerIdMissing() throws Exception {
    Process process = startApplicationWithoutArgs();
    int exitCode = process.waitFor();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

    assertEquals(1, exitCode, "Process should exit with status 1 when broker ID is missing");
    assertTrue(
        output.contains("Missing required argument"),
        () -> "Expected error message about missing broker ID, but was: " + output);
  }

  private Process startApplicationWithoutArgs() throws IOException {
    String javaBin = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String classpath = System.getProperty("java.class.path");

    return new ProcessBuilder(javaBin, "-cp", classpath, ApplicationMain.class.getName())
        .redirectErrorStream(true)
        .start();
  }
}
