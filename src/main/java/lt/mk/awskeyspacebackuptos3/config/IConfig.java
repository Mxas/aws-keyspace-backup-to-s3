package lt.mk.awskeyspacebackuptos3.config;

public interface IConfig {

	String shortName();

	String longName();

	String description();

	String value();

	void set(String v);
}
