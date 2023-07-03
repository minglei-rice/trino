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

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.fileio.ForwardingFileIo;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * A {@link TrinoFileSystem} implementation that reads file from alluxio cache rather than underlying file system.
 */
public class AlluxioFileSystem
        implements TrinoFileSystem
{
    private static final String ALLUXIO_SCHEME = "alluxio";

    private final TrinoFileSystem rawFileSystem;

    public AlluxioFileSystem(TrinoFileSystem rawFileSystem)
    {
        this.rawFileSystem = requireNonNull(rawFileSystem, "rawFileSystem is null");
    }

    @Override
    public TrinoInputFile newInputFile(String path)
    {
        return new AlluxioInputFile(
                rawFileSystem.newInputFile(toAlluxioPath(path)), rawFileSystem.newInputFile(path));
    }

    @Override
    public TrinoInputFile newInputFile(String path, long length)
    {
        return new AlluxioInputFile(
                rawFileSystem.newInputFile(toAlluxioPath(path), length), rawFileSystem.newInputFile(path, length));
    }

    private static String toAlluxioPath(String hdfsPath)
    {
        return ALLUXIO_SCHEME + "://" + new Path(hdfsPath).toUri().getPath();
    }

    @Override
    public TrinoOutputFile newOutputFile(String path)
    {
        return rawFileSystem.newOutputFile(path);
    }

    @Override
    public void deleteFile(String path) throws IOException
    {
        rawFileSystem.deleteFile(path);
    }

    @Override
    public void deleteDirectory(String path) throws IOException
    {
        rawFileSystem.deleteDirectory(path);
    }

    @Override
    public void renameFile(String source, String target) throws IOException
    {
        rawFileSystem.renameFile(source, target);
    }

    @Override
    public FileIterator listFiles(String path) throws IOException
    {
        return rawFileSystem.listFiles(path);
    }

    @Override
    public FileIO toFileIo()
    {
        return new ForwardingFileIo(this);
    }
}
