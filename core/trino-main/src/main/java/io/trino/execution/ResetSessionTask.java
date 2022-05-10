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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ResetSession;

import javax.inject.Inject;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class ResetSessionTask
        implements DataDefinitionTask<ResetSession>
{
    private final Metadata metadata;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public ResetSessionTask(Metadata metadata, SessionPropertyManager sessionPropertyManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "RESET SESSION";
    }

    @Override
    public ListenableFuture<Void> execute(
            ResetSession statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        List<String> parts = statement.getName().getParts();

        boolean isGlobal = "global".equals(parts.get(0));

        if (isGlobal && parts.size() > 3 || !isGlobal && parts.size() > 2) {
            throw semanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", statement.getName());
        }

        Consumer<String> systemPropertyVerifier = property -> {
            if (sessionPropertyManager.getSystemSessionPropertyMetadata(property).isEmpty()) {
                throw semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName());
            }
        };

        BiConsumer<String, String> catalogPropertyVerifier = (catalogNameStr, property) -> {
            CatalogName catalogName = metadata.getCatalogHandle(stateMachine.getSession(), catalogNameStr)
                    .orElseThrow(() -> semanticException(CATALOG_NOT_FOUND, statement, "Catalog '%s' does not exist", catalogNameStr));
            if (sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogName, property).isEmpty()) {
                throw semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName());
            }
        };

        // validate the property name
        if (isGlobal) {
            if (parts.size() == 2) {
                systemPropertyVerifier.accept(parts.get(1));
                sessionPropertyManager.removeRuntimeSystemSessionProperty(parts.get(1));
            }
            else {
                catalogPropertyVerifier.accept(parts.get(1), parts.get(2));
                sessionPropertyManager.removeRuntimeConnectorSessionProperty(metadata.getCatalogHandle(stateMachine.getSession(), parts.get(1)).get(), parts.get(2));
            }

            stateMachine.addResetRuntimeSessionProperty(String.join(".", parts.subList(1, parts.size())));
        }
        else {
            if (parts.size() == 1) {
                systemPropertyVerifier.accept(parts.get(0));
            }
            else {
                catalogPropertyVerifier.accept(parts.get(0), parts.get(1));
            }

            stateMachine.addResetSessionProperties(statement.getName().toString());
        }

        return immediateVoidFuture();
    }
}
