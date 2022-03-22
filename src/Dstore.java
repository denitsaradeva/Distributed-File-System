public class Dstore {

    private int port;
    private int cPort;
    private long timeout;
    private String fileFolder;

    public Dstore(int port, int cPort, long timeout, String fileFolder) {
        this.port = port;
        this.cPort = cPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }
}
