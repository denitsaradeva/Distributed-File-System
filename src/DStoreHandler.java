import java.io.*;
import java.net.Socket;
import java.util.Objects;
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
                while (true) {
//                    if (dStore.isClosed()) {
                    // }
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
                            controller.rebalanceOperationInit();
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
                            System.out.println("TO handle");
                            //TODO
                        }
                    }else{
                        System.out.println("Dstore " + port + " disconnected.");
                        controller.removePort(port);
                        System.out.println("Records deleted.");
                       // File file = new File("DStore" + port + "Files");
//                        for (File current : file.listFiles()) {
//                            current.delete();
//                        }
//                        System.out.println("Files deleted.");
                       // file.delete();

//                        File[] list = file.listFiles();
//                        if (list != null) {
//                            for (File temp : list) {
//                                //recursive delete
//                                temp.delete();
//                               // System.out.println("Visit " + temp);
//                            }
//                        }
//
//                        if (file.delete()) {
//                            System.out.printf("Delete : %s%n", file);
//                        } else {
//                            System.err.printf("Unable to delete file or directory : %s%n", file);
//                        }
//                        System.out.println("Directory deleted.");
                        return;
                    }
                    input = dIn.readLine();
                }
            } catch (IOException e) {
                System.out.println("here");
                e.printStackTrace();
            }
        }).start();

    }
}
