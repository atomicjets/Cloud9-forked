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

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;


/**
 * Hadoop {@code WikipediaPageFactory} for creating language dependent WikipediaPage Objects.
 *
 * @author Peter Exner
 * @author Ferhan Ture
 */
public class WikipediaPageFactory {
  // Wikipedia roughly corresponds to language but sometimes it's not exactlu true (eg. simple - simple English,
  // dialects and expired languages like Serbo-Croatian)
  static final String[] SUPPORTED_WIKIPEDIAS = {
      "ar", "be", "bg", "ca", "ceb", "cs", "de", "el", "en", "es", "fa", "fi", "fr", "gl", "he", "hi", "hr", "hu",
      "hy", "id", "it", "ja", "ka", "ko", "nl", "no", "pl", "pt", "ru", "sh", "simple", "sk", "sl", "sr", "sv",
      "th", "tr", "uk", "ur", "vi", "war", "zh", "zh_yue"
   };

  /**
   * Returns a {@code WikipediaPage} for this {@code language}.
   */
  public static WikipediaPage createWikipediaPage(String language) {
    if (language == null) {
      return new EnglishWikipediaPage();
    }

    if (language.equalsIgnoreCase("en")) {
      return new EnglishWikipediaPage();
    } else if (language.equalsIgnoreCase("sv")) {
      return new SwedishWikipediaPage();
    } else if (language.equalsIgnoreCase("nl")) {
      return new DutchWikipediaPage();
    } else if (language.equalsIgnoreCase("de")) {
      return new GermanWikipediaPage();
    } else if (language.equalsIgnoreCase("fr")) {
      return new FrenchWikipediaPage();
    } else if (language.equalsIgnoreCase("ru")) {
      return new RussianWikipediaPage();
    } else if (language.equalsIgnoreCase("it")) {
      return new ItalianWikipediaPage();
    } else if (language.equalsIgnoreCase("es")) {
      return new SpanishWikipediaPage();
    } else if (language.equalsIgnoreCase("vi")) {
      return new VietnameseWikipediaPage();
    } else if (language.equalsIgnoreCase("pl")) {
      return new PolishWikipediaPage();
    } else if (language.equalsIgnoreCase("ja")) {
      return new JapaneseWikipediaPage();
    } else if (language.equalsIgnoreCase("pt")) {
      return new PortugueseWikipediaPage();
    } else if (language.equalsIgnoreCase("zh")) {
      return new ChineseWikipediaPage();
    } else if (language.equalsIgnoreCase("uk")) {
      return new UkrainianWikipediaPage();
    } else if (language.equalsIgnoreCase("ca")) {
      return new CatalanWikipediaPage();
    } else if (language.equalsIgnoreCase("fa")) {
      return new PersianWikipediaPage();
    } else if (language.equalsIgnoreCase("no")) {
      return new NorwegianWikipediaPage();
    } else if (language.equalsIgnoreCase("fi")) {
      return new FinnishWikipediaPage();
    } else if (language.equalsIgnoreCase("id")) {
      return new IndonesianWikipediaPage();
    } else if (language.equalsIgnoreCase("ar")) {
      return new ArabicWikipediaPage();
    } else if (language.equalsIgnoreCase("sr")) {
      return new SerbianWikipediaPage();
    } else if (language.equalsIgnoreCase("cs")) {
      return new CzechWikipediaPage();
    } else if (language.equalsIgnoreCase("ko")) {
      return new KoreanWikipediaPage();
    } else if (language.equalsIgnoreCase("hi")) {
      return new HindiWikipediaPage();
    } else if (language.equalsIgnoreCase("zh_yue")) {
      return new CantoneseWikipediaPage();
    } else if (language.equalsIgnoreCase("be")) {
      return new BelarusianWikipediaPage();
    } else if (language.equalsIgnoreCase("bg")) {
      return new BulgarianWikipediaPage();
    } else if (language.equalsIgnoreCase("bg")) {
      return new BulgarianWikipediaPage();
    } else if (language.equalsIgnoreCase("ceb")) {
      return new CebuanoWikipediaPage();
    } else if (language.equalsIgnoreCase("el")) {
      return new GreekWikipediaPage();
    } else if (language.equalsIgnoreCase("gl")) {
      return new GalicianWikipediaPage();
    } else if (language.equalsIgnoreCase("he")) {
      return new HebrewWikipediaPage();
    } else if (language.equalsIgnoreCase("hr")) {
      return new CroatianWikipediaPage();
    } else if (language.equalsIgnoreCase("hu")) {
      return new HungarianWikipediaPage();
    } else if (language.equalsIgnoreCase("hy")) {
      return new ArmenianWikipediaPage();
    } else if (language.equalsIgnoreCase("ka")) {
      return new GeorgianWikipediaPage();
    } else if (language.equalsIgnoreCase("sh")) {
      return new SerboCroatianWikipediaPage();
    } else if (language.equalsIgnoreCase("simple")) {
      return new SimpleEnglishWikipediaPage();
    } else if (language.equalsIgnoreCase("sk")) {
      return new SlovakWikipediaPage();
    } else if (language.equalsIgnoreCase("sl")) {
      return new SlovenianWikipediaPage();
    } else if (language.equalsIgnoreCase("th")) {
      return new ThaiWikipediaPage();
    } else if (language.equalsIgnoreCase("th")) {
      return new ThaiWikipediaPage();
    } else if (language.equalsIgnoreCase("tr")) {
      return new TurkishWikipediaPage();
    } else if (language.equalsIgnoreCase("ur")) {
      return new UrduWikipediaPage();
    } else if (language.equalsIgnoreCase("war")) {
      return new WarayWikipediaPage();
    }  else {
      return new EnglishWikipediaPage();
    }
  }

  public static Class<? extends WikipediaPage> getWikipediaPageClass(String language) {
    if (language == null) {
      return EnglishWikipediaPage.class;
    }

    if (language.equalsIgnoreCase("en")) {
      return EnglishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("sv")) {
      return SwedishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("nl")) {
      return DutchWikipediaPage.class;
    } else if (language.equalsIgnoreCase("de")) {
      return GermanWikipediaPage.class;
    } else if (language.equalsIgnoreCase("fr")) {
      return FrenchWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ru")) {
      return RussianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("it")) {
      return ItalianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("es")) {
      return SpanishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("vi")) {
      return VietnameseWikipediaPage.class;
    } else if (language.equalsIgnoreCase("pl")) {
      return PolishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ja")) {
      return JapaneseWikipediaPage.class;
    } else if (language.equalsIgnoreCase("pt")) {
      return PortugueseWikipediaPage.class;
    } else if (language.equalsIgnoreCase("zh")) {
      return ChineseWikipediaPage.class;
    } else if (language.equalsIgnoreCase("uk")) {
      return UkrainianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ca")) {
      return CatalanWikipediaPage.class;
    } else if (language.equalsIgnoreCase("fa")) {
      return PersianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("no")) {
      return NorwegianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("fi")) {
      return FinnishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("id")) {
      return IndonesianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ar")) {
      return ArabicWikipediaPage.class;
    } else if (language.equalsIgnoreCase("sr")) {
      return SerbianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("cs")) {
      return CzechWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ko")) {
      return KoreanWikipediaPage.class;
    } else if (language.equalsIgnoreCase("hi")) {
      return HindiWikipediaPage.class;
    } else if (language.equalsIgnoreCase("zh_yue")) {
      return CantoneseWikipediaPage.class;
    } else if (language.equalsIgnoreCase("be")) {
      return BelarusianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("bg")) {
      return BulgarianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("bg")) {
      return BulgarianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ceb")) {
      return CebuanoWikipediaPage.class;
    } else if (language.equalsIgnoreCase("el")) {
      return GreekWikipediaPage.class;
    } else if (language.equalsIgnoreCase("gl")) {
      return GalicianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("he")) {
      return HebrewWikipediaPage.class;
    } else if (language.equalsIgnoreCase("hr")) {
      return CroatianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("hu")) {
      return HungarianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("hy")) {
      return ArmenianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ka")) {
      return GeorgianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("sh")) {
      return SerboCroatianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("simple")) {
      return SimpleEnglishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("sk")) {
      return SlovakWikipediaPage.class;
    } else if (language.equalsIgnoreCase("sl")) {
      return SlovenianWikipediaPage.class;
    } else if (language.equalsIgnoreCase("th")) {
      return ThaiWikipediaPage.class;
    } else if (language.equalsIgnoreCase("th")) {
      return ThaiWikipediaPage.class;
    } else if (language.equalsIgnoreCase("tr")) {
      return TurkishWikipediaPage.class;
    } else if (language.equalsIgnoreCase("ur")) {
      return UrduWikipediaPage.class;
    } else if (language.equalsIgnoreCase("war")) {
      return WarayWikipediaPage.class;
    } else {
      return EnglishWikipediaPage.class;
    }
  }
}
