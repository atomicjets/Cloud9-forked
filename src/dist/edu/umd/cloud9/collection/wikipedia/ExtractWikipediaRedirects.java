package edu.umd.cloud9.collection.wikipedia;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringEscapeUtils;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool for taking a Wikipedia XML dump file and writing out article titles and ambiguous related titles
 * in a flat text file (article title and related titles, separated by tabs; related titles are '\002' separated).
 *
 * @author Gaurav Ragtah (gaurav.ragtah@lithium.com)
 */

//EXAMPLE REDIRECT PAGE: http://en.wikipedia.org/wiki/Special:Export/Soccer


public class ExtractWikipediaRedirects extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractWikipediaRedirects.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
  }

  private static class WikiRedirectMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {

    private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
    private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");
    private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);
    // Sometimes, URLs bump up against comments e.g., <!-- http://foo.com/-->
    // So remove comments first, since the URL pattern might capture comment terminators.
    private static final Pattern URL = Pattern.compile("http://[^ <]+");
    private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");
    private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>");
    private static final Pattern NEWLINE = Pattern.compile("[\\r\\n]+");
    private static final String SINGLE_SPACE = " ";

    private static final Pattern[] patternsToCleanUp = {LANG_LINKS, REF, HTML_COMMENT, URL, DOUBLE_CURLY, HTML_TAG, NEWLINE};

    private static final String REDIRECT_TITLE_START_TAG_TEXT = "<redirect title=\"";
    private static final String REDIRECT_TITLE_END_TAG_TEXT = "\" />";

    // TODO: The XML field for redirect title is fine, but the redirect page identifier might have additional alternatives
    // for other languages. Investigate at some point.
    // Or maybe just change how redirect pages are identified (boolean): just find the redirect field OR find redirect indicator in text field

    private static final Text title = new Text();
    private static final Text redirectTitle = new Text();

    @Override
    public void map(LongWritable key, WikipediaPage p, Context context) throws IOException, InterruptedException {
      context.getCounter(PageTypes.TOTAL).increment(1);

      if (p.isEmpty()) {
        context.getCounter(PageTypes.EMPTY).increment(1);
      } else if (p.isRedirect()) {
        context.getCounter(PageTypes.REDIRECT).increment(1);

        title.set(p.getTitle());
        String rawXML = p.getRawXML();

        int redirectTitleStart = rawXML.indexOf(REDIRECT_TITLE_START_TAG_TEXT);
        int redirectTitleEnd = rawXML.indexOf(REDIRECT_TITLE_END_TAG_TEXT, redirectTitleStart);

        if (redirectTitleStart == -1) {
          context.getCounter(WikiRedirectMapper.class.getSimpleName(), "NO_REDIRECT_TITLE_START").increment(1);
          return;
        }

        redirectTitle.set(rawXML.substring(redirectTitleStart + 17, redirectTitleEnd).trim());
        context.write(title, redirectTitle);

      } else if (p.isDisambiguation()) {
        context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
      } else if (p.isArticle()) {
        context.getCounter(PageTypes.ARTICLE).increment(1);
        if (p.isStub()) {
          context.getCounter(PageTypes.STUB).increment(1);
        }
      } else {
        context.getCounter(PageTypes.OTHER).increment(1);
      }
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String LANGUAGE_OPTION = "wiki_language";

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

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      LOG.error("Error parsing command line: " + exp.getMessage());
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
      if (!(language.length() == 2 || language.length() == 6)) {
        LOG.error("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - language: " + language);

    Job job = Job.getInstance(getConf());
    job.setJarByClass(ExtractWikipediaRedirects.class);
    job.setJobName(String.format("ExtractWikipediaRedirects[%s: %s, %s: %s, %s: %s]", INPUT_OPTION,
        inputPath, OUTPUT_OPTION, outputPath, LANGUAGE_OPTION, language));

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(WikiRedirectMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public ExtractWikipediaRedirects() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractWikipediaRedirects(), args);
  }
}
