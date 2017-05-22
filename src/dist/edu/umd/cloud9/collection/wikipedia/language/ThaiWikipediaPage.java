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
 * A Swedish page from Wikipedia.
 *
 * @author Nemanja Spasojevic
 */
public class ThaiWikipediaPage extends GenericWikipediaPage {
  /**
   * Language dependent identifiers of disambiguation, redirection, and stub pages.
   */
  private static final String[] LOWER_CASED_REDIRECTS = {"#เปลี่ยนทาง"};
  private static final String DISAMB_PATTERN = "\\{\\{\u0E04\u0E27\u0E32\u0E21\u0E2B\u0E21\u0E32\u0E22\u0E2D\u0E37\u0E48\u0E19|" +
      "\u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E41\u0E01\u0E49\u0E01\u0E33\u0E01\u0E27\u0E21\\}\\}";

  private static final String LANGUAGE_CODE = "th";

  /**
   * Creates an empty <code>ThaiWikipediaPage</code> object.
   */
  public ThaiWikipediaPage() {
    super(LANGUAGE_CODE, LOWER_CASED_REDIRECTS, DISAMB_PATTERN);
  }
}
