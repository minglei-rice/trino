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
package io.trino.plugin.hive.function;

import io.airlift.configuration.Config;
import io.trino.spi.function.Description;

public class ExternalFunctionManagerConfig
{
    private String externalFunctionsStorageDir = "/tmp/functions";

    private String hiveExternalFunctionSchemaDefault = "default";

    public String getExternalFunctionsStorageDir()
    {
        return externalFunctionsStorageDir;
    }

    @Config("external.function.baseDir")
    @Description("the place that all of the things related to the external functions")
    public void setExternalFunctionsStorageDir(String externalFunctionsStorageDir)
    {
        this.externalFunctionsStorageDir = externalFunctionsStorageDir;
    }

    public String getHiveExternalFunctionSchemaDefault()
    {
        return hiveExternalFunctionSchemaDefault;
    }

    @Config("external.function.hive.schema")
    @Description("in which schema to find the function by default, for example: default.b_parse_url(...)")
    public void setHiveExternalFunctionSchemaDefault(String hiveExternalFunctionSchemaDefault)
    {
        this.hiveExternalFunctionSchemaDefault = hiveExternalFunctionSchemaDefault;
    }
}
