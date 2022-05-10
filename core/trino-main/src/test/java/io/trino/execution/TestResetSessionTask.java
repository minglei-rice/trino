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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Catalog;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.ResetSession;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.createBogusTestingCatalog;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.transaction.InMemoryTransactionManager.createTestTransactionManager;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestResetSessionTask
{
    private static final String CATALOG_NAME = "catalog";
    private ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final Metadata metadata;
    private final SessionPropertyManager sessionPropertyManager;

    public TestResetSessionTask()
    {
        CatalogManager catalogManager = new CatalogManager();
        transactionManager = createTestTransactionManager(catalogManager);
        accessControl = new AllowAllAccessControl();

        metadata = createTestMetadataManager(transactionManager, new FeaturesConfig());
        sessionPropertyManager = new SessionPropertyManager();

        sessionPropertyManager.addSystemSessionProperty(stringProperty(
                "foo",
                "test property",
                null,
                false));

        sessionPropertyManager.addSystemSessionProperty(stringProperty(
                "sys",
                "system property should be override at runtime",
                null,
                false));

        Catalog bogusTestingCatalog = createBogusTestingCatalog(CATALOG_NAME);
        sessionPropertyManager.addConnectorSessionProperties(bogusTestingCatalog.getConnectorCatalogName(), ImmutableList.of(
                stringProperty(
                        "baz",
                        "test property",
                        null,
                        false),
                stringProperty(
                        "sess",
                        "system property should be override at runtime",
                        null,
                        false)));

        catalogManager.registerCatalog(bogusTestingCatalog);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void test()
    {
        Session session = testSessionBuilder(sessionPropertyManager)
                .setSystemProperty("foo", "bar")
                .setCatalogSessionProperty(CATALOG_NAME, "baz", "blah")
                .build();

        assertNull(session.getSystemProperties().get("sys"));
        assertNull(session.getConnectorProperties().get(new CatalogName(CATALOG_NAME)));

        sessionPropertyManager.addRuntimeSystemSessionProperty("sys", "override");
        sessionPropertyManager.addRuntimeConnectorSessionProperty(new CatalogName(CATALOG_NAME), "sess", "override");

        Session sessionWithRuntimeProperties = testSessionBuilder(sessionPropertyManager).build();
        assertEquals(sessionWithRuntimeProperties.getSystemProperties().get("sys"), "override");
        assertEquals(sessionWithRuntimeProperties.getConnectorProperties().get(new CatalogName(CATALOG_NAME)).get("sess"), "override");

        QueryStateMachine stateMachine = QueryStateMachine.begin(
                "reset foo",
                Optional.empty(),
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                transactionManager,
                accessControl,
                executor,
                metadata,
                WarningCollector.NOOP,
                Optional.empty());

        getFutureValue(new ResetSessionTask(metadata, sessionPropertyManager).execute(
                new ResetSession(QualifiedName.of(CATALOG_NAME, "baz")),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));

        Set<String> sessionProperties = stateMachine.getResetSessionProperties();
        assertEquals(sessionProperties, ImmutableSet.of("catalog.baz"));

        getFutureValue(new ResetSessionTask(metadata, sessionPropertyManager).execute(
                new ResetSession(QualifiedName.of("global", "sys")),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));

        getFutureValue(new ResetSessionTask(metadata, sessionPropertyManager).execute(
                new ResetSession(QualifiedName.of("global", CATALOG_NAME, "sess")),
                stateMachine,
                emptyList(),
                WarningCollector.NOOP));

        Set<String> resetRuntimeSessionProperties = stateMachine.getResetRuntimeSessionProperties();
        assertFalse(resetRuntimeSessionProperties.contains("global.sys"));
        assertFalse(resetRuntimeSessionProperties.contains(String.join(".", "global", CATALOG_NAME, "sess")));
        assertEquals(resetRuntimeSessionProperties.size(), 2);

        List<SessionPropertyManager.SessionPropertyValue> values =
                sessionPropertyManager.getAllSessionProperties(session, ImmutableMap.of(CATALOG_NAME, new CatalogName(CATALOG_NAME)));
        assertTrue(values.stream().anyMatch(v -> v.getPropertyName().equals("sys")));
        assertEquals(values.stream().filter(v -> v.getPropertyName().equals("sys")).findFirst().get().getValue(), "");
        assertTrue(values.stream().anyMatch(v -> v.getPropertyName().equals("sess")));
        assertEquals(values.stream().filter(v -> v.getPropertyName().equals("sess")).findFirst().get().getValue(), "");

        assertEquals(sessionWithRuntimeProperties.getSystemProperties().get("sys"), "override");
        assertEquals(sessionWithRuntimeProperties.getConnectorProperties().get(new CatalogName(CATALOG_NAME)).get("sess"), "override");
    }
}
