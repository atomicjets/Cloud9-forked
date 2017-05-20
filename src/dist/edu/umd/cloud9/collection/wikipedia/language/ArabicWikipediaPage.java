/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.collection.wikipedia.language;

/**
 * An Arabic page from Wikipedia.
 *
 * @author Ferhan Ture
 * @author Nemanja Spasojevic
 */
public class ArabicWikipediaPage extends GenericWikipediaPage {
  /**
   * Language dependent identifiers of disambiguation, redirection, and stub pages.
   */
  private static final String[] LOWER_CASED_REDIRECTS = {"#\u062A\u062D\u0648\u064A\u0644"};
  private static final String DISAMB_PATTERN = "\\{\\{\u062A\u0648\u0636\u064A\u062D\\}\\}";
  private static final String LANGUAGE_CODE = "ar";

  /**
   * Creates an empty <code>ArabicWikipediaPage</code> object.
   */
  public ArabicWikipediaPage() {
    super(LANGUAGE_CODE, LOWER_CASED_REDIRECTS,DISAMB_PATTERN);
  }
}
