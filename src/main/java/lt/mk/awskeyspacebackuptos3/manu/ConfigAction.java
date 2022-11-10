package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.Validator;
import java.io.File;
import java.util.function.Consumer;
import lt.mk.awskeyspacebackuptos3.config.ConfigsDocs;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;

public class ConfigAction extends ActionInThread {

	private final ConfigurationHolder configurationHolder;

	public ConfigAction(ConfigurationHolder configurationHolder) {
		super("Configuring (press Enter to skip config)", "Configure management");
		this.configurationHolder = configurationHolder;
	}

	@Override
	public void execute() {
		for (ConfigsDocs c : ConfigsDocs.values()) {
			read(c.getDescription(),c.getGet().apply(configurationHolder), v-> c.getSet().accept(configurationHolder,v));
		}
	}

	private void read(String message, String current, Consumer<String> set) {

		read(message, current, set, null);
	}

	private void read(String message, String current, Consumer<String> set, Validator<String> validator) {
		println("Current property value: " + (current == null ? "" : current));
		String value = this.prompt(message + ": ", String.class, validator);
		if (isNotBlank(value)) {
			set.accept(value);
		}
	}

	private boolean isNotBlank(String value) {
		return value != null && value.trim().length() > 0;
	}
}
