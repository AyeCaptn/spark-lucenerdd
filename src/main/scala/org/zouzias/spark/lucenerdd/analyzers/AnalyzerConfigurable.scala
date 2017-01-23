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

import java.io.Reader
import java.util.regex.Pattern

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents
import org.apache.lucene.analysis.core.{LowerCaseFilter, WhitespaceTokenizer}
import org.apache.lucene.analysis.miscellaneous.{ASCIIFoldingFilter, LengthFilter, TrimFilter}
import org.apache.lucene.analysis.pattern.{PatternReplaceCharFilter, PatternReplaceFilter}
import org.zouzias.spark.lucenerdd.config.Configurable

/**
  * Lucene Analyzer loader via configuration
  */

class CustomAnalyzer extends Analyzer with Serializable {

  override def createComponents(fieldName: String): TokenStreamComponents = {
    val tokenizer = new WhitespaceTokenizer()
    val asciiFoldingFilter = new ASCIIFoldingFilter(tokenizer)
    val alphaNumericFilter = new PatternReplaceFilter(asciiFoldingFilter,
      Pattern.compile("[^A-Za-z0-9]"), "", true)
    val lowerCaseFilter = new LowerCaseFilter(alphaNumericFilter)
    val trimFilter = new TrimFilter(lowerCaseFilter)
    val lengthFilter = new LengthFilter(trimFilter, 2, Int.MaxValue)
    new TokenStreamComponents(tokenizer, lengthFilter)
  }

  override def initReader(fieldName: String, reader: Reader): Reader = {
    new PatternReplaceCharFilter(Pattern.compile("\\(.+\\)"), "", reader)
  }
}

trait AnalyzerConfigurable extends Configurable {

  protected val Analyzer: Analyzer = {
    new CustomAnalyzer()
  }
}
