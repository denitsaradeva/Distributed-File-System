import java.io.*;
import java.net.Socket;
import java.util.concurrent.*;

public class ClientHandler extends Thread {
    private final Controller controller;
    private final BufferedReader dIn;
    private final PrintWriter dOut;

    private final Socket client;

    public ClientHandler(Socket client, Controller controller) throws IOException {
        this.client = client;
        this.controller = controller;
        InputStream inputStream = client.getInputStream();
        OutputStream outputStream = client.getOutputStream();
        this.dIn = new BufferedReader(new InputStreamReader(inputStream));
        this.dOut = new PrintWriter(outputStream);
    }

    @Override
    public void run() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    //  System.out.println("From hCandler: waiting for input.");
                    String input = dIn.readLine();
                    if (input != null) {
                        System.out.println("From clientHandler " + input);
                        controller.receiveMessages(input, dOut, client);
                        System.out.println("From clientHandler: receive complete.");
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
