/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide;

import static glide.TestUtilities.commonClusterClientConfig;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import glide.api.GlideClusterClient;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Tests for abandoned callbacks in the JNI bridge. */
@Timeout(30)
public class AbandonedCallbackTests {

    private static GlideClusterClient client;

    @BeforeAll
    @SneakyThrows
    public static void setup() {
        // Disable timeout to prove callback abandonment
        client =
                GlideClusterClient.createClient(commonClusterClientConfig().requestTimeout(0).build())
                        .get();
    }

    @AfterAll
    @SneakyThrows
    public static void teardown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @SneakyThrows
    public void testAbandonedCallback() {
        String testKey = "test_abandon_callback_key";
        CompletableFuture<String> future = client.get(testKey);

        try {
            // If callback is abandoned, operation will timeout
            future.get(2, TimeUnit.SECONDS);
            fail("Command succeeded when instrumentation should have triggered error");
        } catch (TimeoutException e) {
            fail("Future timed out because callback was abandoned");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() != null, "ExecutionException should have a cause");
            System.out.println("  Error: " + e.getCause().getMessage());
        }
    }

    @Test
    @SneakyThrows
    public void testCallbacksAlwaysComplete() {
        String testKey = "test_key_callback";
        CompletableFuture<String> future = client.get(testKey);

        try {
            // Operation should pass or fail, but not timeout
            String result = future.get(3, TimeUnit.SECONDS);
            assertTrue(result == null || result.isEmpty(), "Non-existent key should return null");
        } catch (TimeoutException e) {
            fail("Future timed out because callback was abandoned");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() != null, "ExecutionException should have a cause");
        }
    }
}
