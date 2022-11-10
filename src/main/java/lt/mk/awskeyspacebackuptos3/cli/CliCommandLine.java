package lt.mk.awskeyspacebackuptos3.cli;

import java.util.function.BiConsumer;
import lt.mk.awskeyspacebackuptos3.config.ConfigsDocs;
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
			for (ConfigsDocs c : ConfigsDocs.values()) {
				setIfProvided(cmd, c.name(), c.getSet());
			}
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

	private void setIfProvided(CommandLine cmd, String opt, BiConsumer<ConfigurationHolder, String> set) {
		if (cmd.hasOption(opt)) {
			set.accept(config, cmd.getOptionValue(opt));
		}
	}


	private static Options buildOptions() {
		Options options = new Options();
		for (ConfigsDocs c : ConfigsDocs.values()) {
			put(options, c.name(), c.getOptionLong(), c.getDescription());
		}
		return options;
	}

	private static void put(Options options, String option, String longOption, String description) {
		options.addOption(Option.builder(option).longOpt(longOption)
				.argName("")
				.hasArg()
				.required(false)
				.desc(description).build());
	}
}
