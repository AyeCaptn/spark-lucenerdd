/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.zouzias.spark.lucenerdd.analyzers

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.{LetterTokenizer, LowerCaseFilter, WhitespaceTokenizer}
import org.zouzias.spark.lucenerdd.config.Configurable

/**
  * Lucene Analyzer loader via configuration
  */

class CustomAnalyzer extends Analyzer with Serializable {

  override def createComponents(fieldName: String): TokenStreamComponents = {
    val tokenizer = new WhitespaceTokenizer()
    val lowerCaseFilter = new LowerCaseFilter(tokenizer)
    new TokenStreamComponents(tokenizer, lowerCaseFilter)
  }
}

trait AnalyzerConfigurable extends Configurable {

  protected val Analyzer: Analyzer = {
    new CustomAnalyzer()
  }
}
