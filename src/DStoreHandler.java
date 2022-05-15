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
    private String input;

    private BufferedReader dIn;

    //private String[] message;

//    public DStoreHandler(Socket dStore, Controller controller, long timeout, CountDownLatch countDownLatch, String[] message) {
//        this.dStore = dStore;
//        this.controller = controller;
//        this.timeout = timeout;
//        this.countDownLatch = countDownLatch;
//      //  this.message = message;
//    }

    public DStoreHandler(String input, BufferedReader bf, Socket dStore, Controller controller, long timeout, CountDownLatch countDownLatch, String[] message) {
        this.dStore = dStore;
        this.input = input;
        this.dIn = bf;
        this.controller = controller;
        this.timeout = timeout;
        this.countDownLatch = countDownLatch;
        //  this.message = message;
    }

    @Override
    public void run() {
        new Thread(() -> {
            try {
                //  PrintWriter dOut = new PrintWriter(dStore.getOutputStream());
                while (true) {
//                    if (countDownLatch == null) {
//                        System.out.println("ajjsjsik");
//                        int port = Integer.parseInt(message[1]);
//                        System.out.println("DStore with port " + port + " is connected.");
//                        this.port = port;
//                        controller.addPort(port, dStore);
//                        //  controller.rebalanceOperationInit();
//                        System.out.println(32);
//                    }
                    if (input != null) {
                        if (input.contains("JOIN")) {
                            int port = Integer.parseInt(input.split(" ")[1]);
                            System.out.println("DStore with port " + port + " is connected.");
                            this.port = port;
                            controller.addPort(port, dStore);
                            countDownLatch.countDown();
                        } else if (input.contains("STORE_ACK")) {
                            System.out.println("STORE_ACK received from DStore.");
                            controller.writeToClient("Store", "STORE_COMPLETE",
                                    input.split(" ")[1], port);
                            System.out.println("STORE_COMPLETE message sent to client.");
                        } else if (input.contains("REMOVE_ACK")) {
                            controller.writeToClient("Remove", "REMOVE_COMPLETE",
                                    input.split(" ")[1], port);
                        } else if (input.contains("LIST")) {
                            //controller.notifyIndexCopy();
//                        String[] splitInput = input.split(" ");
//                        String result = "";
//                        for (int i = 1; i < splitInput.length; i++) {
//                            result += splitInput[i] + " ";
//                        }
//                        controller.rebalanceOperation(result);
                            //System.out.println(input.split(" ")[1]);
                        } else {
                            //TODO
                        }
                    }
                    input = dIn.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

    }
}
