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
 * A Persian page from Wikipedia.
 *
 * @author Gaurav Ragtah
 * @author Nemanja Spasojevic
 */
public class PersianWikipediaPage extends GenericWikipediaPage {
  /**
   * Language dependent identifiers of disambiguation, redirection, and stub pages.
   */
  private static final String[] LOWER_CASED_REDIRECTS = {
      "#\u062A\u063A\u06CC\u06CC\u0631\u0645\u0633\u06CC\u0631",
      "#\u062A\u063A\u06CC\u06CC\u0631_\u0645\u0633\u06CC\u0631"
  };
  private static final String DISAMB_PATTERN = "\\{\\{\u0627\u0628\u0647\u0627\u0645\u200C\u0632\u062F\u0627\u06CC\u06CC\\}\\}";
  private static final String LANGUAGE_CODE = "fa";

  /**
   * Creates an empty <code>PersianWikipediaPage</code> object.
   */
  public PersianWikipediaPage() {
    super(LANGUAGE_CODE, LOWER_CASED_REDIRECTS,DISAMB_PATTERN);
  }
}
