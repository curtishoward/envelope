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

import java.util.Iterator;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.cloudera.labs.envelope.load.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.typesafe.config.Config;

/**
 *  Align schema with hive table
 */
public class HiveAlignDeriver implements Deriver, ProvidesAlias {

  public static final String HIVE_TABLE_CONFIG_NAME = "table";

  private Config config;

  @Override
  public void configure(Config config) {
    this.config = config;
  }

  @Override
  public Dataset<Row> derive(Map<String, Dataset<Row>> dependencies) throws Exception {
    String hiveTable;

    if (config.hasPath(HIVE_TABLE_CONFIG_NAME)) {
      hiveTable = config.getString(HIVE_TABLE_CONFIG_NAME);
    }
    else {
      throw new RuntimeException("Hive align deriver table name not provided. Use '" + HIVE_TABLE_CONFIG_NAME + "'.");
    }

    if (dependencies.size() != 1) {
      throw new RuntimeException("Hive align deriver requires exactly 1 dependency");
    }

    Map.Entry<String, Dataset<Row>> dependency = dependencies.entrySet().iterator().next();
    Set<String> dependencyColsHash = new HashSet<String>(Arrays.asList(dependency.getValue().schema().fieldNames()));
    List<String> alignCols = Arrays.asList(Contexts.getSparkSession().sql("select * from " + hiveTable + " limit 0").schema().fieldNames());

    List<String> queryFields = new ArrayList<String>();
    for (String col : alignCols) {
      if (dependencyColsHash.contains(col)) {
        queryFields.add(col);
      }
      else {
        queryFields.add("NULL as " + col);
      }
    }

    String alignQuery = "SELECT " + String.join(", ", queryFields) + " FROM " + dependency.getKey();
    Dataset<Row> derived = Contexts.getSparkSession().sql(alignQuery);

    return derived;
  }

  @Override
  public String getAlias() {
    return "hivealign";
  }
}
