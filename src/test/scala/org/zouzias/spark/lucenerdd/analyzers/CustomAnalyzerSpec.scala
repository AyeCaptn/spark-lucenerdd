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

import java.io.StringReader

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.{Analyzer, TokenStream}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class CustomAnalyzerSpec extends FlatSpec with Matchers {

  def testAnalyzer(analyzer: Analyzer, input: String, output: List[String]): Unit = {
    val ts: TokenStream = analyzer.tokenStream("dummy", new StringReader(input))
    ts.hasAttribute(classOf[CharTermAttribute]) shouldBe true
    val termAtt: CharTermAttribute = ts.getAttribute(classOf[CharTermAttribute])
    val tokens: ArrayBuffer[String] = ArrayBuffer[String]()
    ts.reset()
    while (ts.incrementToken())
      tokens.append(termAtt.toString)
    ts.end()
    ts.close()
    tokens.toList should equal (output)
  }

  val analyzer = new CustomAnalyzer()

  "CustomAnalyzer" should "remove braces and their content" in {
    testAnalyzer(analyzer, "(this is content)", List[String]())
    testAnalyzer(analyzer, "this (this is content) is content",
      List[String]("this", "is", "content"))
  }

  "CustomAnalyzer" should "paste together symbol separated letters" in {
    testAnalyzer(analyzer, "T.H.I.S.", List[String]("this"))
  }

  "CustomAnalyzer" should "remove single letters" in {
    testAnalyzer(analyzer, "a, b, cc, ddd", List[String]("cc", "ddd"))
  }

  "CustomAnalyzer" should "remove all kinds of symbols" in {
    testAnalyzer(analyzer, "(!ddns^jz!&$!", List[String]("ddnsjz"))
  }
}
