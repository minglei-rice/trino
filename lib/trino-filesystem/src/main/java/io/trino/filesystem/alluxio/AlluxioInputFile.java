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
package io.trino.filesystem.alluxio;

import io.airlift.log.Logger;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class AlluxioInputFile
        implements TrinoInputFile
{
    private static final Logger log = Logger.get(AlluxioInputFile.class);

    private final TrinoInputFile alluxioInputFile;
    private final TrinoInputFile rawInputFile;
    private boolean useAlluxioCache;

    public AlluxioInputFile(TrinoInputFile alluxioInputFile, TrinoInputFile rawInputFile)
    {
        this.alluxioInputFile = requireNonNull(alluxioInputFile, "alluxioInputFile is null");
        this.rawInputFile = requireNonNull(rawInputFile, "rawInputFile is null");
        this.useAlluxioCache = true;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        if (useAlluxioCache) {
            try {
                return alluxioInputFile.newInput();
            }
            catch (Exception e) {
                handleAlluxioFailure(e);
            }
        }
        return rawInputFile.newInput();
    }

    @Override
    public long length()
            throws IOException
    {
        if (useAlluxioCache) {
            try {
                return alluxioInputFile.length();
            }
            catch (Exception e) {
                handleAlluxioFailure(e);
            }
        }
        return rawInputFile.length();
    }

    @Override
    public long modificationTime()
            throws IOException
    {
        if (useAlluxioCache) {
            try {
                return alluxioInputFile.modificationTime();
            }
            catch (Exception e) {
                handleAlluxioFailure(e);
            }
        }
        return rawInputFile.modificationTime();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        if (useAlluxioCache) {
            try {
                return alluxioInputFile.exists();
            }
            catch (Exception e) {
                handleAlluxioFailure(e);
            }
        }
        return rawInputFile.exists();
    }

    @Override
    public String location()
    {
        if (useAlluxioCache) {
            return alluxioInputFile.location();
        }
        return rawInputFile.location();
    }

    // When Alluxio failure occurs, log a warning and no longer use Alluxio cache for this file
    private void handleAlluxioFailure(Exception e)
    {
        log.warn("Try not to use Alluxio cache for path %s due to Alluxio failure", alluxioInputFile.location(), e);
        useAlluxioCache = false;
    }
}
