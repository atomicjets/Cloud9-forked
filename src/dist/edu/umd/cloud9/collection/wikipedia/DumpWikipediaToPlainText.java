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

package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for taking a Wikipedia XML dump file and spits out articles in a flat text file (article
 * title and content, separated by a tab).
 *
 * @author Jimmy Lin
 * @author Peter Exner
 * @author Gaurav Ragtah (gaurav.ragtah@lithium.com)
 */
public class DumpWikipediaToPlainText extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DumpWikipediaToPlainText.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
  };

  private static class MyMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
    private static final Text articleId = new Text();
    private static final Text articleTitleAndContent = new Text();


    @Override
    public void map(LongWritable key, WikipediaPage p, Context context)
        throws IOException, InterruptedException {
      String contentFormat = context.getConfiguration().get("wiki.content_format");
      context.getCounter(PageTypes.TOTAL).increment(1);

      if (p.isRedirect()) {
        context.getCounter(PageTypes.REDIRECT).increment(1);
      } else if (p.isDisambiguation()) {
        context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
      } else if (p.isEmpty()) {
        context.getCounter(PageTypes.EMPTY).increment(1);
      } else if (p.isArticle()) {
        context.getCounter(PageTypes.ARTICLE).increment(1);

        if (p.isStub()) {
          context.getCounter(PageTypes.STUB).increment(1);
        }

        System.out.println("Processing : " + p.getDocid() +  " via " + contentFormat);
        articleId.set(p.getDocid());
        if (contentFormat == null || contentFormat.equals("HTML")) {
          articleTitleAndContent.set(
              p.getTitle().replaceAll("[\\r\\n]+", " ")
                  + "\t"
                  + p.getContent().replaceAll("[\\r\\n]+", " ")
          );
        } else if (contentFormat.equals("HTML")) {
          articleTitleAndContent.set(p.getDisplayContent().replaceAll("[\\r\\n\\t]+", " "));
        } else if (contentFormat.equals("WIKI")) {
          articleTitleAndContent.set(
              p.getWikiMarkup().replaceAll("[\\r\\n\\t]+", " ") +
                  " {{Wiki Title|" + p.getTitle() + "}}" +
                  " {{Wiki Page Id|" + p.getDocid() + "}}"
          );
        }



        context.write(articleId, articleTitleAndContent);
      } else {
        context.getCounter(PageTypes.OTHER).increment(1);
      }
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String LANGUAGE_OPTION = "wiki_language";
  private static final String CONTENT_FORMAT_OPTION = "content_format";


  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|nl|de|fr|ru|it|es|vi|pl|ja|pt|zh|uk|ca|fa|no|fi|id|ar|sr|ko|hi|zh_yue|cs|tr").hasArg()
        .withDescription("two-letter or six-letter language code").create(LANGUAGE_OPTION));
    options.addOption(OptionBuilder.withArgName("TEXT|HTML|WIKI").hasArg()
        .withDescription("Output Content Type TEXT, HTML, WIKI").create(CONTENT_FORMAT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String language = "en"; // Assume "en" by default.
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if(!(language.length() == 2 || language.length() == 6)){
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String contentFormat = "TEXT"; // Assume "TEXT" by default.
    if (cmdline.hasOption(CONTENT_FORMAT_OPTION)) {
      contentFormat = cmdline.getOptionValue(CONTENT_FORMAT_OPTION);
      if (!contentFormat.equals("TEXT") &&
          !contentFormat.equals("HTML") &&
          !contentFormat.equals("WIKI")) {

        System.err.println("Error: \"" + contentFormat + "\" unknown content type!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path  : " + outputPath);
    LOG.info(" - language     : " + language);
    LOG.info(" - content_type : " + contentFormat);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJarByClass(DumpWikipediaToPlainText.class);
    job.setJobName(String.format("DumpWikipediaToPlainText[%s: %s, %s: %s, %s: %s, %s: %s]",
        INPUT_OPTION, inputPath,
        OUTPUT_OPTION, outputPath,
        LANGUAGE_OPTION, language,
        CONTENT_FORMAT_OPTION, contentFormat));

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }
    if (contentFormat != null) {
      job.getConfiguration().set("wiki.content_format", contentFormat);
    }
    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public DumpWikipediaToPlainText() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DumpWikipediaToPlainText(), args);
  }
}

/**
<<<<<<< HEAD
 Example of how to run this utility:

 ssh <herever lib is >
 cd ~/test_lib_cloud9/

 scp thunder@jobs-aa-sched1:~/thunder/bin/scripts/r_wikipedia.sh klout@sci1:~/bin/

 ssh klout@sci1
 cd /home/research/lib

 LANGUAGE=de
 hadoop jar cloud9-1.5.0-klout.jar \
   edu.umd.cloud9.collection.wikipedia.DumpWikipediaToPlainText \
   -libjars bliki-core-3.0.16.jar,commons-lang3-3.1jarBAK,commons-lang3-3.2.jar \
   -input /data/prod/inputs/wikipedia/20150716/xmldump/${LANGUAGE}wiki-latest-pages-articles.xml \
   -output /data/hive/research/test_wiki_text/20150716/${LANGUAGE} \
   -wiki_language $LANGUAGE \
   -content_format WIKI
 */