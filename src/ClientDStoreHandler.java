import java.io.*;
import java.net.Socket;

public class ClientDStoreHandler extends Thread {
    private final Socket dStoreSocket;
    private final Dstore dStore;

    private final InputStream inputStream;

    private final OutputStream outputStream;

    private final BufferedReader dIn;
    private final PrintWriter dOut;

    public ClientDStoreHandler(Socket dStoreSocket, Dstore dStore) throws IOException {
        this.dStore = dStore;
        this.dStoreSocket = dStoreSocket;
        this.inputStream = dStoreSocket.getInputStream();
        this.outputStream = dStoreSocket.getOutputStream();
        this.dIn = new BufferedReader(new InputStreamReader(inputStream));
        this.dOut = new PrintWriter(outputStream);

    }

    @Override
    public void run() {
        new Thread(() -> {
            while (true) {
                try {
                    String input = dIn.readLine();
                    if (input != null) {
                        System.out.println("From handler " + input);
                        dStore.receiveMessages(input, this);
                        System.out.println("From handler: receive complete.");
                    }
                    if (dStoreSocket.isClosed()) {
                        return;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }

            }

        }).start();

    }

    public Socket getdStoreSocket() {
        return dStoreSocket;
    }

    public Dstore getdStore() {
        return dStore;
    }

    public BufferedReader getdIn() {
        return dIn;
    }

    public PrintWriter getdOut() {
        return dOut;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }
}
