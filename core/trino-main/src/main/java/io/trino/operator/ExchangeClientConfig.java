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
package io.trino.operator;

import io.airlift.configuration.Config;
import io.airlift.http.client.HttpClientConfig;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ExchangeClientConfig
{
    private DataSize maxBufferSize = DataSize.of(32, MEGABYTE);
    private int concurrentRequestMultiplier = 3;
    private Duration maxErrorDuration = new Duration(5, TimeUnit.MINUTES);
    private DataSize maxResponseSize = new HttpClientConfig().getMaxContentLength();
    private int clientThreads = 25;
    private int pageBufferClientMaxCallbackThreads = 25;
    private boolean acknowledgePages = true;
    private Duration idleTimeout = new Duration(30, SECONDS);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private int maxConnectionsPerServer = 250;
    private DataSize maxContentLength = DataSize.of(32, MEGABYTE);

    @NotNull
    public DataSize getMaxBufferSize()
    {
        return maxBufferSize;
    }

    @Config("exchange.max-buffer-size")
    public ExchangeClientConfig setMaxBufferSize(DataSize maxBufferSize)
    {
        this.maxBufferSize = maxBufferSize;
        return this;
    }

    @Min(1)
    public int getConcurrentRequestMultiplier()
    {
        return concurrentRequestMultiplier;
    }

    @Config("exchange.concurrent-request-multiplier")
    public ExchangeClientConfig setConcurrentRequestMultiplier(int concurrentRequestMultiplier)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        return this;
    }

    @Deprecated
    public Duration getMinErrorDuration()
    {
        return maxErrorDuration;
    }

    @Deprecated
    @Config("exchange.min-error-duration")
    public ExchangeClientConfig setMinErrorDuration(Duration minErrorDuration)
    {
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getMaxErrorDuration()
    {
        return maxErrorDuration;
    }

    @Config("exchange.max-error-duration")
    public ExchangeClientConfig setMaxErrorDuration(Duration maxErrorDuration)
    {
        this.maxErrorDuration = maxErrorDuration;
        return this;
    }

    @NotNull
    @MinDataSize("1MB")
    public DataSize getMaxResponseSize()
    {
        return maxResponseSize;
    }

    @Config("exchange.max-response-size")
    public ExchangeClientConfig setMaxResponseSize(DataSize maxResponseSize)
    {
        this.maxResponseSize = maxResponseSize;
        return this;
    }

    @Min(1)
    public int getClientThreads()
    {
        return clientThreads;
    }

    @Config("exchange.client-threads")
    public ExchangeClientConfig setClientThreads(int clientThreads)
    {
        this.clientThreads = clientThreads;
        return this;
    }

    @Min(1)
    public int getPageBufferClientMaxCallbackThreads()
    {
        return pageBufferClientMaxCallbackThreads;
    }

    @Config("exchange.page-buffer-client.max-callback-threads")
    public ExchangeClientConfig setPageBufferClientMaxCallbackThreads(int pageBufferClientMaxCallbackThreads)
    {
        this.pageBufferClientMaxCallbackThreads = pageBufferClientMaxCallbackThreads;
        return this;
    }

    public boolean isAcknowledgePages()
    {
        return acknowledgePages;
    }

    @Config("exchange.acknowledge-pages")
    public ExchangeClientConfig setAcknowledgePages(boolean acknowledgePages)
    {
        this.acknowledgePages = acknowledgePages;
        return this;
    }

    public Duration getIdleTimeout()
    {
        return idleTimeout;
    }

    @Config("exchange.http-client.idle-timeout")
    public ExchangeClientConfig setIdleTimeout(Duration idleTimeout)
    {
        this.idleTimeout = idleTimeout;
        return this;
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("exchange.http-client.request-timeout")
    public ExchangeClientConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public int getMaxConnectionsPerServer()
    {
        return maxConnectionsPerServer;
    }

    @Config("exchange.http-client.max-connections-per-server")
    public ExchangeClientConfig setMaxConnectionsPerServer(int maxConnectionsPerServer)
    {
        this.maxConnectionsPerServer = maxConnectionsPerServer;
        return this;
    }

    public DataSize getMaxContentLength()
    {
        return maxContentLength;
    }

    @Config("exchange.http-client.max-content-length")
    public ExchangeClientConfig setMaxContentLength(DataSize maxContentLength)
    {
        this.maxContentLength = maxContentLength;
        return this;
    }
}
