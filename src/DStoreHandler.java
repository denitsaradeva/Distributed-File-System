import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.*;

public class DStoreHandler extends Thread {
    private final Socket dStore;
    private final Controller controller;
    private long timeout;
    private final CountDownLatch countDownLatch;
    private int port;

    public DStoreHandler(Socket dStore, Controller controller, long timeout, CountDownLatch countDownLatch) {
        this.dStore = dStore;
        this.controller = controller;
        this.timeout = timeout;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        new Thread(() -> {
            try {
                BufferedReader dIn = new BufferedReader(new InputStreamReader(dStore.getInputStream()));
                //  PrintWriter dOut = new PrintWriter(dStore.getOutputStream());
                while (true) {
                    String input = dIn.readLine();
                    if (input.contains("JOIN")) {
                        int port = Integer.parseInt(input.split(" ")[1]);
                        System.out.println("DStore with port " + port + " is connected.");
                        this.port = port;
                        controller.addPort(port, dStore);
                        countDownLatch.countDown();
                    } else if (input.contains("STORE_ACK")) {
                        controller.writeToClient("Store", "STORE_COMPLETE",
                                input.split(" ")[1], port);
                    } else if (input.contains("REMOVE_ACK")) {
                        controller.writeToClient("Remove", "REMOVE_COMPLETE",
                                input.split(" ")[1], port);
                    } else {
                        //Ignore and log
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }
}
