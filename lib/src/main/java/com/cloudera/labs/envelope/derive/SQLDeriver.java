/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.labs.envelope.derive;

import java.io.InputStream;
import java.io.InputStreamReader;
//import java.util.Map;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.typesafe.config.Config;

/**
 * Execute Spark SQL on Datasets.
 */
public class SQLDeriver implements Deriver, ProvidesAlias {

  public static final String QUERY_LITERAL_CONFIG_NAME = "query.literal";
  public static final String QUERY_FILE_CONFIG_NAME = "query.file";
  public static final String SCHEMA_CONFIG_NAME = "align.schema";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    String query;

    if (config.hasPath(QUERY_LITERAL_CONFIG_NAME)) {
      query = config.getString(QUERY_LITERAL_CONFIG_NAME);
    }
    else if (config.hasPath(QUERY_FILE_CONFIG_NAME)) {
      query = hdfsFileAsString(config.getString(QUERY_FILE_CONFIG_NAME));
    }
    else {
      throw new RuntimeException("SQL deriver query not provided. Use '" + QUERY_LITERAL_CONFIG_NAME + "' or '" + QUERY_FILE_CONFIG_NAME + "'.");
    }

    Dataset<Row> derived = Contexts.getSparkSession().sql(query);

    if (config.hasPath(SCHEMA_CONFIG_NAME)) {
      List<String> alignCols = new ArrayList<String>();

      if (dependencies.containsKey(config.getString(SCHEMA_CONFIG_NAME)))  {
        alignCols = Arrays.asList(dependencies.get(config.getString(SCHEMA_CONFIG_NAME)).schema().fieldNames());
      }
      else {
        throw new RuntimeException("The \"" + SCHEMA_CONFIG_NAME + "\" deriver named: \"" + config.getString(SCHEMA_CONFIG_NAME) + 
                                   "\" was either not found or not included in this deriver's dependencies list.");
      }
      Set<String> dependencyColsHash = new HashSet<String>(Arrays.asList(derived.schema().fieldNames())); 
      List<String> queryFields = new ArrayList<String>();
      for (String col : alignCols) {
        if (dependencyColsHash.contains(col)) {
          queryFields.add(col);
        }
        else {
          queryFields.add("NULL as " + col);
        }
      }
      String fieldString = "SELECT " + String.join(", ", queryFields) + " FROM (" + query + ")";
      System.out.println(">>>>>>>>>>>>>>>>>>> " + fieldString);
    }

    return derived;
  }

  private String hdfsFileAsString(String hdfsFile) throws Exception {
    String contents = null;

    FileSystem fs = FileSystem.get(new Configuration());
    InputStream stream = fs.open(new Path(hdfsFile));
    InputStreamReader reader = new InputStreamReader(stream, Charsets.UTF_8);
    contents = CharStreams.toString(reader);
    reader.close();
    stream.close();

    return contents;
  }

  @Override
  public String getAlias() {
    return "sql";
  }
}
