package lt.mk.awskeyspacebackuptos3.cli;

import java.util.function.Consumer;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CliCommandLine {

	private final String[] args;

	private final ConfigurationHolder config;
	private final Options options;

	public CliCommandLine(String[] args, ConfigurationHolder configHolder) {
		this.args = args;
		this.options = buildOptions();
		this.config = configHolder;
	}

	public void parse() {

		try {
			CommandLine cmd = new DefaultParser().parse(options, args);

			setIfProvided(cmd, "m", v -> config.menu.disableMenu = !Boolean.parseBoolean(v));
			setIfProvided(cmd, "f", v -> config.keyspace.awsKeyspaceDriverConfigPath = v);
			setIfProvided(cmd, "t", v -> config.keyspace.table = v);
			setIfProvided(cmd, "k", v -> config.keyspace.keyspace = v);
			setIfProvided(cmd, "q", v -> config.keyspace.query = v);
			setIfProvided(cmd, "fsrez", v -> config.fs.storeTo = v);
			setIfProvided(cmd, "s3r", v -> config.s3.region = v);
			setIfProvided(cmd, "s3b", v -> config.s3.bucket = v);
			setIfProvided(cmd, "s3f", v -> config.s3.folder = v);

		} catch (ParseException e) {
			System.out.println(e.getMessage());
			printUsage();
			System.exit(0);
		}
	}

	public void printUsage() {
		HelpFormatter helper = new HelpFormatter();
		helper.printHelp("Usage:", options);
	}

	private void setIfProvided(CommandLine cmd, String opt, Consumer<String> set) {
		if (cmd.hasOption(opt)) {
			set.accept(cmd.getOptionValue(opt));
		}
	}


	private static Options buildOptions() {
		Options options = new Options();

		options.addOption(Option.builder("m").longOpt("menu")
				.argName("menu")
				.hasArg()
				.required(false)
				.desc("Use interactive menu").build());

		options.addOption(Option.builder("f").longOpt("cql-config-file")
				.argName("cql-config-file")
				.hasArg()
				.required(false)
				.desc("AWS Keyspace connection *.conf file path").build());

		options.addOption(Option.builder("t").longOpt("table")
				.argName("table")
				.hasArg()
				.required(false)
				.desc("AWS Keyspace table").build());

		options.addOption(Option.builder("k").longOpt("keyspace")
				.argName("keyspace")
				.hasArg()
				.required(false)
				.desc("AWS Keyspace storage 'keyspace'").build());

		options.addOption(Option.builder("q").longOpt("query")
				.argName("query")
				.hasArg()
				.required(false)
				.desc("AWS Keyspace data query").build());

		options.addOption(Option.builder("fsrez").longOpt("file-to-store")
				.argName("file-to-store")
				.hasArg()
				.required(false)
				.desc("FS local result file path").build());

		options.addOption(Option.builder("s3r").longOpt("s3-region")
				.argName("s3-region")
				.hasArg()
				.required(false)
				.desc("AWS Region").build());

		options.addOption(Option.builder("s3b").longOpt("s3-bucket")
				.argName("s3-bucket")
				.hasArg()
				.required(false)
				.desc("AWS S3 bucket name").build());

		options.addOption(Option.builder("s3f").longOpt("s3-folder")
				.argName("s3-folder")
				.hasArg()
				.required(false)
				.desc("AWS S3 folder name in bucket (prefix before <...>/<keyspace>/<table>/file_name.csv result object)").build());

		return options;
	}
}
