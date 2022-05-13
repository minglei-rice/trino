/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.base.Joiner;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.TestingTicker;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStaticMetastoreLocator
{
    private static final ThriftMetastoreClient DEFAULT_CLIENT = createFakeMetastoreClient();
    private static final ThriftMetastoreClient FALLBACK_CLIENT = createFakeMetastoreClient();

    private static final String DEFAULT_URI = "thrift://default:8080";
    private static final String FALLBACK_URI = "thrift://fallback:8090";
    private static final String FALLBACK2_URI = "thrift://fallback2:8090";

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris(Joiner.on(',').join(DEFAULT_URI, FALLBACK_URI, FALLBACK2_URI));

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK = new StaticMetastoreConfig()
            .setMetastoreUris(DEFAULT_URI);

    private static final StaticMetastoreConfig CONFIG_WITH_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris(Joiner.on(',').join(DEFAULT_URI, FALLBACK_URI, FALLBACK2_URI))
            .setMetastoreUsername("presto");

    private static final StaticMetastoreConfig CONFIG_WITHOUT_FALLBACK_WITH_USER = new StaticMetastoreConfig()
            .setMetastoreUris(DEFAULT_URI)
            .setMetastoreUsername("presto");

    private static final Map<String, Optional<ThriftMetastoreClient>> CLIENTS = ImmutableMap.of(DEFAULT_URI, Optional.of(DEFAULT_CLIENT), FALLBACK_URI, Optional.of(FALLBACK_CLIENT));

    @Test
    public void testDefaultHiveMetastore()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.of(DEFAULT_CLIENT)));
        assertEqualHiveClient(locator.createMetastoreClient(Optional.empty()), DEFAULT_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastore()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.empty(), FALLBACK_URI, Optional.of(FALLBACK_CLIENT)));
        assertEqualHiveClient(locator.createMetastoreClient(Optional.empty()), FALLBACK_CLIENT);
    }

    @Test
    public void testFallbackHiveMetastoreFails()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK, ImmutableMap.of());
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080, fallback:8090, fallback2:8090]");
    }

    @Test
    public void testMetastoreFailedWithoutFallback()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITHOUT_FALLBACK, ImmutableMap.of(DEFAULT_URI, Optional.empty()));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreWithHiveUser()
            throws TException
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITH_FALLBACK_WITH_USER, ImmutableMap.of(DEFAULT_URI, Optional.empty(), FALLBACK_URI, Optional.empty(), FALLBACK2_URI, Optional.of(FALLBACK_CLIENT)));
        assertEqualHiveClient(locator.createMetastoreClient(Optional.empty()), FALLBACK_CLIENT);
    }

    @Test
    public void testMetastoreFailedWithoutFallbackWithHiveUser()
    {
        MetastoreLocator locator = createMetastoreLocator(CONFIG_WITHOUT_FALLBACK_WITH_USER, ImmutableMap.of(DEFAULT_URI, Optional.empty()));
        assertCreateClientFails(locator, "Failed connecting to Hive metastore: [default:8080]");
    }

    @Test
    public void testFallbackHiveMetastoreOnTimeOut()
            throws TException
    {
        Set<ThriftMetastoreClient> clients = CLIENTS.values().stream().map(Optional::get).collect(Collectors.toSet());
        StaticMetastoreLocator cluster = (StaticMetastoreLocator) createMetastoreLocator(CONFIG_WITH_FALLBACK, CLIENTS);

        ThriftMetastoreClient metastoreClient1 = cluster.createMetastoreClient(Optional.empty());

        assertContainsHiveClient(clients, metastoreClient1);

        assertGetTableException(metastoreClient1);
        clients.remove(((FailureAwareThriftMetastoreClient) metastoreClient1).getDelegate());

        ThriftMetastoreClient metastoreClient2 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient2);

        assertGetTableException(metastoreClient2);
    }

    @Test
    public void testFallbackHiveMetastoreOnAllTimeOut()
            throws TException
    {
        Set<ThriftMetastoreClient> clients = CLIENTS.values().stream().map(Optional::get).collect(Collectors.toSet());
        StaticMetastoreLocator cluster = (StaticMetastoreLocator) createMetastoreLocator(CONFIG_WITH_FALLBACK, CLIENTS);

        ThriftMetastoreClient metastoreClient1 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient1);

        for (int i = 0; i < 20; ++i) {
            assertGetTableException(metastoreClient1);
        }
        clients.remove(((FailureAwareThriftMetastoreClient) metastoreClient1).getDelegate());

        ThriftMetastoreClient metastoreClient2 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient2);

        assertGetTableException(metastoreClient2);

        // Still get FALLBACK_CLIENT because DEFAULT_CLIENT failed more times before and therefore longer backoff
        ThriftMetastoreClient metastoreClient3 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient3);
    }

    @Test
    public void testStickToFallbackAfterBackoff()
            throws TException
    {
        Set<ThriftMetastoreClient> clients = CLIENTS.values().stream().map(Optional::get).collect(Collectors.toSet());

        TestingTicker ticker = new TestingTicker();
        StaticMetastoreLocator cluster = (StaticMetastoreLocator) createMetastoreLocator(CONFIG_WITH_FALLBACK, CLIENTS, ticker);

        ticker.increment(10, NANOSECONDS);
        ThriftMetastoreClient metastoreClient1 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient1);

        assertGetTableException(metastoreClient1);
        clients.remove(((FailureAwareThriftMetastoreClient) metastoreClient1).getDelegate());

        ticker.increment(10, NANOSECONDS);
        ThriftMetastoreClient metastoreClient2 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient2);

        // even after backoff for DEFAULT_CLIENT passes we should stick to client which we saw working correctly most recently
        ticker.increment(StaticMetastoreLocator.Backoff.MAX_BACKOFF, NANOSECONDS);
        ThriftMetastoreClient metastoreClient3 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient3);
    }

    @Test
    public void testReturnsToDefaultClientAfterErrorOnFallback()
            throws TException
    {
        Set<ThriftMetastoreClient> clients = CLIENTS.values().stream().map(Optional::get).collect(Collectors.toSet());

        TestingTicker ticker = new TestingTicker();
        StaticMetastoreLocator cluster = (StaticMetastoreLocator) createMetastoreLocator(CONFIG_WITH_FALLBACK, CLIENTS, ticker);

        ticker.increment(10, NANOSECONDS);
        ThriftMetastoreClient metastoreClient1 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient1);
        assertGetTableException(metastoreClient1);
        clients.remove(((FailureAwareThriftMetastoreClient) metastoreClient1).getDelegate());

        ticker.increment(10, NANOSECONDS);
        ThriftMetastoreClient metastoreClient2 = cluster.createMetastoreClient(Optional.empty());
        assertContainsHiveClient(clients, metastoreClient2);
        assertGetTableException(metastoreClient2);

        ticker.increment(10, NANOSECONDS);
        ThriftMetastoreClient metastoreClient3 = cluster.createMetastoreClient(Optional.empty());
        assertEqualHiveClient(metastoreClient3, metastoreClient1);
    }

    @Test
    public void testReturnRandomClient()
    {
        ImmutableMap.Builder<String, Optional<ThriftMetastoreClient>> testClientMapBuilder = ImmutableMap.builder();
        testClientMapBuilder.put(FALLBACK2_URI, Optional.of(createFakeMetastoreClient()));
        testClientMapBuilder.putAll(CLIENTS);
        ImmutableMap<String, Optional<ThriftMetastoreClient>> testClientMap = testClientMapBuilder.build();
        TestingTicker ticker = new TestingTicker();
        StaticMetastoreLocator cluster = (StaticMetastoreLocator) createMetastoreLocator(CONFIG_WITH_FALLBACK, testClientMap, ticker);
        Set<ThriftMetastoreClient> testClients = testClientMap.values().stream().map(Optional::get).collect(Collectors.toSet());
        for (int i = 0; i < Byte.MAX_VALUE && !testClients.isEmpty(); i++) {
            testClients.removeIf(expectedClient -> {
                try {
                    return expectedClient == ((FailureAwareThriftMetastoreClient) cluster.createMetastoreClient(Optional.empty())).getDelegate();
                }
                catch (TException e) {
                    return false;
                }
            });
        }
        assertTrue(testClients.isEmpty(), "Should fetch all clients!");
    }

    private static void assertGetTableException(ThriftMetastoreClient client)
    {
        assertThatThrownBy(() -> client.getTable("foo", "bar"))
                .isInstanceOf(TException.class)
                .hasMessageContaining("Read timeout");
    }

    private static void assertCreateClientFails(MetastoreLocator locator, String message)
    {
        assertThatThrownBy(() -> locator.createMetastoreClient(Optional.empty()))
                .hasCauseInstanceOf(TException.class)
                .hasMessage(message);
    }

    private static MetastoreLocator createMetastoreLocator(StaticMetastoreConfig config, Map<String, Optional<ThriftMetastoreClient>> clients)
    {
        return createMetastoreLocator(config, clients, Ticker.systemTicker());
    }

    private static MetastoreLocator createMetastoreLocator(StaticMetastoreConfig config, Map<String, Optional<ThriftMetastoreClient>> clients, Ticker ticker)
    {
        return new StaticMetastoreLocator(config, new ThriftMetastoreAuthenticationConfig(), new MockThriftMetastoreClientFactory(clients), ticker);
    }

    private static ThriftMetastoreClient createFakeMetastoreClient()
    {
        return new MockThriftMetastoreClient()
        {
            @Override
            public Table getTable(String dbName, String tableName)
                    throws TException
            {
                throw new TException(new SocketTimeoutException("Read timeout"));
            }
        };
    }

    private void assertEqualHiveClient(ThriftMetastoreClient actual, ThriftMetastoreClient expected)
    {
        if (actual instanceof FailureAwareThriftMetastoreClient) {
            actual = ((FailureAwareThriftMetastoreClient) actual).getDelegate();
        }
        if (expected instanceof FailureAwareThriftMetastoreClient) {
            expected = ((FailureAwareThriftMetastoreClient) expected).getDelegate();
        }
        assertEquals(actual, expected);
    }

    private void assertContainsHiveClient(Set<ThriftMetastoreClient> clients, ThriftMetastoreClient expected)
    {
        ThriftMetastoreClient actual = ((FailureAwareThriftMetastoreClient) expected).getDelegate();
        assertTrue(clients.contains(actual));
    }
}
