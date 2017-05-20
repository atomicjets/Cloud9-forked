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

import java.lang.String;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import org.glassfish.grizzly.utils.ArraySet;

/**
 * A Generic page from Wikipedia.
 *
 * @author Nemanja Spasojevic
 *
 * Based on EnglishWikipediaPage authored by:
 *
 * @author Peter Exner
 * @author Ferhan Ture
 * @author Gaurav Ragtah
 */
public class GenericWikipediaPage extends WikipediaPage {
  /**
   * Language dependent identifiers of disambiguation, redirection, and stub pages.
   */


  public static final String NO_DISAMBIGUATION_DEFINED = "\\{\\{disambig\\s*\\|?\\s*.*?\\}\\}"; // USe english
  // but reality is that one will not work. However one can look if rules get updated in page
  // https://<LANGUAGE_CODE>.wikipedia.org/wiki/Template:Disambiguation
  // Eg.
  //   https://en.wikipedia.org/wiki/Template:Disambiguation
  //   https://sr.wikipedia.org/wiki/Template:Disambiguation
  // At the moment disambiguation are nice to have so not really looking into
  // finding for each undocumented language rules.

  private static final String IDENTIFIER_STUB_TEMPLATE = "stub}}";
  private static final String IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE = "Wikipedia:Stub";


  private Set<String> redirectIndentifiers_;
  protected final Pattern disambiguationPattern_;
  private final String languageCode_;

  /**
   * Creates an empty <code>GenericWikipediaPage</code> object.
   */
  public GenericWikipediaPage(String languageCode,
                              String[] redirectIndentifiers,
                              String disambiguationPattern) {
    super();
    disambiguationPattern_ = Pattern.compile(disambiguationPattern, Pattern.CASE_INSENSITIVE);
    languageCode_ = languageCode;

    // Add acceptable redirects.
    redirectIndentifiers_ = new HashSet<String>();
    for (String identifier : redirectIndentifiers) {
      redirectIndentifiers_.add(identifier.toLowerCase().trim());
    }
    redirectIndentifiers_.add("#redirect");
  }

  @Override
  protected void processPage(String s) {
    this.language = languageCode_;

    // parse out title
    int start = s.indexOf(XML_START_TAG_TITLE);
    int end = s.indexOf(XML_END_TAG_TITLE, start);
    this.title = StringEscapeUtils.unescapeHtml(s.substring(start + 7, end));

    // determine if article belongs to the article namespace
    start = s.indexOf(XML_START_TAG_NAMESPACE);
    end = s.indexOf(XML_END_TAG_NAMESPACE);
    this.isArticle = s.substring(start + 4, end).trim().equals("0");

    // parse out the document id
    start = s.indexOf(XML_START_TAG_ID);
    end = s.indexOf(XML_END_TAG_ID);
    this.mId = s.substring(start + 4, end);

    // parse out actual text of article
    this.textStart = s.indexOf(XML_START_TAG_TEXT);
    this.textEnd = s.indexOf(XML_END_TAG_TEXT, this.textStart);

    this.disambPattern = disambiguationPattern_;

    // determine if article is a disambiguation, redirection, and/or stub page.
    Matcher matcher = disambPattern.matcher(page);
    this.isDisambig = matcher.find();
    this.isStub = s.indexOf(IDENTIFIER_STUB_TEMPLATE, this.textStart) != -1 ||
                  s.indexOf(IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE) != -1;

    this.isRedirect = false;
    for (String redirectIndentifier : redirectIndentifiers_) {
      // Based on previous code but I do not think it's robust to spaces, etc.
      // It assumes perfect
      String candidateRedirectIdentifier = s.substring(
          this.textStart + XML_START_TAG_TEXT.length(),
          this.textStart + XML_START_TAG_TEXT.length() + redirectIndentifier.length()).toLowerCase().trim();

      this.isRedirect = candidateRedirectIdentifier.equalsIgnoreCase(redirectIndentifier);
      // If redirect than we are done otherwise keep trying.
      if (this.isRedirect) break;
    }

  }
}
