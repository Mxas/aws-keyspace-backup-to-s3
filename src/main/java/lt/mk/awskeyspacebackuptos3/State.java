package lt.mk.awskeyspacebackuptos3;

public class State {

	public static boolean shutdown = false;

	public static boolean isRunning(){
		return !shutdown;
	}

}
