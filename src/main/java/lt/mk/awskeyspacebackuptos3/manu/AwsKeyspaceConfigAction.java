package lt.mk.awskeyspacebackuptos3.manu;

import io.bretty.console.view.ActionView;
import io.bretty.console.view.Validator;
import java.io.File;
import java.util.function.Consumer;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder;
import lt.mk.awskeyspacebackuptos3.config.ConfigurationHolder.AwsKeyspaceConf;
import lt.mk.awskeyspacebackuptos3.keyspace.CqlSessionProvider;

public class AwsKeyspaceConfigAction extends ActionInThread {

	private final ConfigurationHolder configurationHolder;
	private final CqlSessionProvider cqlSessionProvider;

	public AwsKeyspaceConfigAction(ConfigurationHolder configurationHolder, CqlSessionProvider cqlSessionProvider) {
		super("Configuring AWS Keyspace (press Enter to skip config)", "Configure AWS Keyspace");
		this.configurationHolder = configurationHolder;
		this.cqlSessionProvider = cqlSessionProvider;
	}

	@Override
	public void execute() {
		AwsKeyspaceConf keyspace = configurationHolder.keyspace;
		read("Please enter AWS Keyspace config file path", keyspace.awsKeyspaceDriverConfigPath, v -> keyspace.awsKeyspaceDriverConfigPath = v, validateFile());
		read("Please enter AWS Keyspace 'keyspace' name", keyspace.keyspace, v -> keyspace.keyspace = v);
		read("Please enter AWS Keyspace table name", keyspace.table, v -> keyspace.table = v);
		read("Please enter AWS Keyspace query", keyspace.query, v -> keyspace.query = v);
	}

	private Validator<String> validateFile() {
		return s -> {
			if (isNotBlank(s)) {
				File file = new File(s);
				boolean valis = file.isFile() && file.exists();
				if (valis) {
					cqlSessionProvider.close();
				}
				return valis;
			}
			return true;
		};
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
