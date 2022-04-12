import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;

public class Controller implements Runnable {

    //index map -> file <-> port, size
    // TODO : message - ex: processing ?
    private ConcurrentHashMap<String, ArrayList<Integer>> index;
    // private ArrayList<Socket> dStoreSockets;

    //The set of all current DStores
    private HashMap<Integer, Socket> portsAndSockets;

    //The socket responsible for the controller's communication
    private final ServerSocket ss;
    private BufferedReader clientReader;
    private PrintWriter clientWriter;
    int storeCount;
    int removeCount;
    //  private Socket socket;

    //Mandatory fields
    int cPort;
    int R;
    long timeout;
    long rebalancePeriod;

    public Controller(int cPort, int R, long timeout, long rebalancePeriod) throws IOException {
        this.portsAndSockets = new HashMap<>();
        this.index = new ConcurrentHashMap<>();
        // this.dStoreSockets = new ArrayList<>();
        this.cPort = cPort;
        this.storeCount = 0;
        this.removeCount = 0;
        this.R = R;
        this.timeout = timeout;
        this.clientReader = null;
        this.clientWriter = null;
        this.rebalancePeriod = rebalancePeriod;
        this.ss = new ServerSocket(cPort);
    }


    public static void main(String[] args) {

        //Initialize the variables from input
        int cPort = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        long timeout = Long.parseLong(args[2]);
        long rebalancePeriod = Long.parseLong(args[3]);

        //Creating the Controller
        try {
            Controller controller = new Controller(cPort, R, timeout, rebalancePeriod);
            controller.run();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void run() {

        Socket dStore = null;

        //Connecting the DStores
        CountDownLatch countDownLatch = new CountDownLatch(R);
        ExecutorService executorService = Executors.newFixedThreadPool(R);
        for (int i = 0; i < R; i++) {
            try {
                dStore = ss.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Thread a = new DStoreHandler(dStore, this, timeout, countDownLatch);
            executorService.submit(a);
        }
        boolean result = false;

        try {
            result = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();
        if (result) {
            System.out.println("All DStored have successfully connected.");
        } else {
            System.out.println("Timeout has occurred.");
        }

        //Connecting the client
        try {
            Socket clientSocket = ss.accept();
            clientReader = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));
            clientWriter = new PrintWriter(clientSocket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter finalOut = clientWriter;
        BufferedReader finalIn = clientReader;
        new Thread(() -> {
            String received;
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    received = finalIn.readLine();
                    if (received != null) {
                        String[] input = received.split(" ");
                        if (input[0].equals("LIST")) {
                            finalOut.println("LIST " + getFilesAsString());
                            finalOut.flush();
                        } else if (input[0].equals("STORE")) {
                            if (index.containsKey(input[1])) {
                                finalOut.println("ERROR_FILE_ALREADY_EXISTS");
                                finalOut.flush();
                            } else {
                                storeCount = portsAndSockets.size();
                                index.put(input[1], new ArrayList<>());
                                ArrayList<Integer> current = new ArrayList<>();
                                current.add(Integer.parseInt(input[2]));
                                index.replace(input[1], current);
                                finalOut.println("STORE_TO " + getPortsAsString());
                                finalOut.flush();
                            }
//                            if (index.get(input[1]).size() == 1) {
//                                index.remove(input[1]);
//                            }
                        } else if (input[0].equals("REMOVE")) {
                            if (index.containsKey(input[1])) {
                                int count = 0;
                                removeCount = index.get(input[1]).size() - 1;
                                ArrayList<Integer> copyOfIndex = new ArrayList<>(index.get(input[1]));
                                for (Integer port : copyOfIndex) {
                                    //Send "REMOVE filename" message to ports
                                    if (count != 0) {
                                        Socket current = portsAndSockets.get(port);
                                        PrintWriter printWriter = new PrintWriter(current.getOutputStream());
                                        printWriter.println("REMOVE " + input[1]);
                                        printWriter.flush();
                                    }
                                    count++;
                                }
                            } else {
                                finalOut.println("ERROR_FILE_DOES_NOT_EXIST");
                                finalOut.flush();
                                //Handle error
                            }
                        } else if (input[0].equals("LOAD")) {
                            if (!index.containsKey(input[1])) {
                                finalOut.println("ERROR_FILE_DOES_NOT_EXIST");
                                finalOut.flush();
                            }
                            ArrayList<Integer> ports = index.get(input[1]);
                            finalOut.println("LOAD_FROM " + ports.get(1) + " " + ports.get(0));
                            finalOut.flush();
                        } else if (input[0].equals("RELOAD")) {
                            //TODO
                        } else {
                            //Ignore and log
                        }

                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private String getFilesAsString() {
        StringBuilder result = new StringBuilder();
        for (String s : index.keySet()) {
            result.append(s).append(" ");
        }
        return result.toString();
    }

    public String getPortsAsString() {
        StringBuilder result = new StringBuilder();
        for (Integer port : getPorts()) {
            result.append(port).append(" ");
        }
        return result.toString();
    }

    public ArrayList<Integer> getPorts() {
        return new ArrayList<>(portsAndSockets.keySet());
    }

    public void addPort(int port, Socket socket) {
        if (portsAndSockets.containsKey(port)) {
            //Handle error
        } else {
            this.portsAndSockets.put(port, socket);
        }
    }

    public void writeToClient(String operation, String message, String operationVar, int port) {
        if (operation.equals("Store")) {
            storeCount--;
            ArrayList<Integer> current = index.get(operationVar);
            current.add(port);
            index.replace(operationVar, current);
            if (storeCount == 0) {
                clientWriter.println(message);
                clientWriter.flush();
            }
        } else if (operation.equals("Remove")) {
            removeCount--;
            int position = 0;
            for (Integer portNum : index.get(operationVar)) {
                if (portNum == port) {
                    break;
                } else {
                    position++;
                }
            }
            if (index.get(operationVar).contains(port)) {
                index.get(operationVar).remove(position);
            }
            if (index.get(operationVar).size() == 1) {
                index.remove(operationVar);
            }
            if (removeCount == 0) {
                clientWriter.println(message);
                clientWriter.flush();
            }
        }
    }

    public String readFromClient() {
        String message = "";
        try {
            message = clientReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return message;
    }

    public void addToIndex(String filename, int port, int size) {
        ArrayList<Integer> details = new ArrayList<>();
        details.add(port);
        details.add(size);
        if (index.containsKey(filename)) {
            //Throw exception - already exists - failure handling
        } else {
            this.index.put(filename, details);
        }
    }
}
