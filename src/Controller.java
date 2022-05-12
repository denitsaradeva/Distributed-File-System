import java.io.*;
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
    // private BufferedReader clientReader;
    private PrintWriter clientPrinter;
    int storeCount;
    int removeCount;

    private ConcurrentHashMap<Integer, ArrayList<String>> transformedIndex;
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
        this.clientPrinter = null;
        // this.clientWriter = null;
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
//        synchronized (this) {
//            rebalanceOperationInit();
//        }
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                rebalanceOperationInit();
//            }
//        }, 0, rebalancePeriod);

        Socket dStore = null;

        //Connecting the DStores
        CountDownLatch countDownLatch = new CountDownLatch(R);
        //  ExecutorService executorService = Executors.newFixedThreadPool(R);
        for (int i = 0; i < R; i++) {
            try {
                dStore = ss.accept();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] flag = new String[0];

            new DStoreHandler(dStore, this, timeout, countDownLatch, flag).start();
            // executorService.submit(a);
        }
        boolean result = false;

        try {
            result = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //   executorService.shutdown();
        if (result) {
            System.out.println("All DStored have successfully connected.");
        } else {
            System.out.println("Timeout has occurred.");
        }

        //Client comm
        new Thread(() -> {
            while (true) {
                try {
                    Socket currentSocket = ss.accept();
                    OutputStream out = currentSocket.getOutputStream();
                    clientPrinter = new PrintWriter(out);
                    new ClientHandler(currentSocket, this).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }

    private synchronized void storeOperation(String[] input, PrintWriter finalOut) {
        if (index.containsKey(input[1])) {
            finalOut.println("ERROR_FILE_ALREADY_EXISTS");
            finalOut.flush();
        } else {
            storeCount = 1;
            index.put(input[1], new ArrayList<>());
            ArrayList<Integer> current = new ArrayList<>();
            current.add(Integer.parseInt(input[2]));
            index.replace(input[1], current);

            StringBuilder portsToStore = new StringBuilder();
            HashSet<Integer> copyOfPorts = new HashSet<>(portsAndSockets.keySet());
            for (int i = 0; i < R; i++) {
                Random random = new Random();
                boolean success = false;
                while (!success) {
                    int randIndex = random.nextInt(portsAndSockets.size());
                    int port = getPort(randIndex);
                    if (copyOfPorts.contains(port)) {
                        portsToStore.append(port).append(" ");
                        copyOfPorts.remove(port);
                        success = true;
                    }
                }
            }
//            for (int i = 0; i < 1; i++) {
//                Random random = new Random();
//                boolean success = false;
//                //   while (!success) {
//                int randIndex = random.nextInt(portsAndSockets.size());
//                int port = getPort(randIndex);
//                if (copyOfPorts.contains(port)) {
//                    portsToStore.append(port).append(" ");
//                    copyOfPorts.remove(port);
//                    success = true;
//                }
//                //  }
//            }

            finalOut.println("STORE_TO " + portsToStore);
            //  finalOut.println("STORE_TO " + getPortsAsString());
            finalOut.flush();
            System.out.println("STORE_TO message sent to Client.");
        }
    }

    private int getPort(int randIndex) {
        int count = 0;
        int result = 0;
        for (Integer port : portsAndSockets.keySet()) {
            if (count == randIndex) {
                result = port;
            }
            count++;
        }
        return result;
    }

    private synchronized void removeOperation(String[] input) {
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void rebalanceOperationInit() {
        System.out.println("Starting rebalance operation...");
        transformIndex();
        if (transformedIndex.size() == 0) {
            return;
        }

        int N = portsAndSockets.size();
        int F = index.size();
        int lowerBound = (int) Math.floor((R * F * 1.0) / (N * 1.0));
        int upperBound = (int) Math.ceil((R * F * 1.0) / (N * 1.0));

        //portToSend - filesToSend - filesToRemove
        HashMap<Integer, ArrayList<String>> messagesToSend = new HashMap<>();
        System.out.println(22);
        while (!rebalancedBounds(lowerBound, upperBound) || !rebalancedRepl()) {
            //  while (!rebalancedBounds(lowerBound, upperBound)) {
            int low = 0;
            int high = 0;
            int count = 0;
            int reqMeet = 0;
            for (Map.Entry<Integer, ArrayList<String>> entry : transformedIndex.entrySet()) {
                if (entry.getValue().size() > lowerBound && high == 0) {
                    high = entry.getKey();
                } else if (entry.getValue().size() < upperBound && low == 0) {
                    low = entry.getKey();
                } else {
                    reqMeet = entry.getKey();
                }
                if (count == transformedIndex.size() - 1) {
                    if (low != 0 && high == 0) {
                        high = reqMeet;
                    } else if (low == 0 && high != 0) {
                        low = reqMeet;
                    }
                }
                count++;
            }
            if (low != 0 && high != 0) {
                int NFilesToSend = 0;
                int NFilesToRemove = 0;
                ArrayList<String> filesToSend = new ArrayList<>();
                ArrayList<String> filesToRemove = new ArrayList<>();
                for (String file : transformedIndex.get(high)) {
                    //Checks whether the destination already has the given file
                    if (!transformedIndex.get(low).contains(file)) {
                        //Checks whether the destination has space for files
                        if (transformedIndex.get(low).size() < upperBound) {
                            //Checks whether the sender can remove the file after sending it
                            if (transformedIndex.get(high).size() > lowerBound) {
                                filesToSend.add(file);
                                filesToRemove.add(file);
                                NFilesToSend++;
                                NFilesToRemove++;
                            } else {
                                filesToSend.add(file);
                                NFilesToSend++;
                                //TODO
                            }

                            //Preparing the rebalance request
                            StringBuilder result = new StringBuilder(NFilesToSend + " ");
                            for (String fileToSend : filesToSend) {
                                result.append(fileToSend).append(" 1 ").append(low).append(" ");
                            }
                            result.append(NFilesToRemove).append(" ");
                            for (String fileToRemove : filesToRemove) {
                                result.append(fileToRemove).append(" ");
                            }
                            String toSend = "REBALANCE " + result;

                            //Sending the rebalance request
                            Socket current = portsAndSockets.get(high);
                            try {
                                PrintWriter printWriter = new PrintWriter(current.getOutputStream());
                                printWriter.println(toSend);
                                printWriter.flush();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                            //Updating info
//                            int highIndex = index.get(file).indexOf(high);
//                            index.get(file).remove(highIndex);

                            for (String fileT : filesToSend) {
                                index.get(fileT).add(low);
                            }

                            if (filesToRemove.size() != 0) {
                                for (String fileR : filesToRemove) {
                                    index.get(fileR).remove((Integer) high);
                                }
                            }
                            transformIndex();
                        }
                    }
                }
            }
            for (Map.Entry<Integer, ArrayList<String>> entry : messagesToSend.entrySet()) {
                StringBuilder sb = new StringBuilder();
                sb.append(entry.getKey()).append(" ");
                for (String message : entry.getValue()) {
                    sb.append(message).append(" ");
                }
                System.out.println(sb);
            }
        }
    }

    private synchronized boolean rebalancedBounds(int lowerBound, int upperBound) {
        boolean rebalanced = true;
        for (Map.Entry<Integer, ArrayList<String>> entry : transformedIndex.entrySet()) {
            if (entry.getValue().size() < lowerBound || entry.getValue().size() > upperBound) {
                rebalanced = false;
                break;
            }
        }
        return rebalanced;
    }

    private synchronized boolean rebalancedRepl() {
        boolean rebalanced = true;
        for (Map.Entry<String, ArrayList<Integer>> entry : index.entrySet()) {
            if (!(entry.getValue().size() == (R + 1))) {
                rebalanced = false;
                break;
            }
        }
        return rebalanced;
    }

    private String getFilesAsString() {
        StringBuilder result = new StringBuilder();
        for (String s : index.keySet()) {
            result.append(s).append(" ");
        }
        //TODO remove duplicates
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
                clientPrinter.println(message);
                clientPrinter.flush();
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
                clientPrinter.println(message);
                clientPrinter.flush();
            }
        }
    }

    private void transformIndex() {
        transformedIndex = new ConcurrentHashMap<>();
        for (Map.Entry<String, ArrayList<Integer>> entry : index.entrySet()) {
            for (int i = 1; i < entry.getValue().size(); i++) {
                if (!transformedIndex.containsKey(entry.getValue().get(i))) {
                    transformedIndex.put(entry.getValue().get(i), new ArrayList<>());
                    transformedIndex.get(entry.getValue().get(i)).add(entry.getKey());
                } else {
                    transformedIndex.get(entry.getValue().get(i)).add(entry.getKey());
                }
            }
        }
        for (Integer port : portsAndSockets.keySet()) {
            if (!transformedIndex.containsKey(port)) {
                transformedIndex.put(port, new ArrayList<>());
            }
        }
    }

    public void receiveMessages(String input, PrintWriter clientWriter) {
        if (input != null) {
            String[] received = input.split(" ");
            if (received[0].equals("LIST")) {
                clientWriter.println("LIST " + getFilesAsString());
                clientWriter.flush();
            } else if (received[0].equals("STORE")) {
                storeOperation(received, clientWriter);
            } else if (received[0].equals("REMOVE")) {
                if (index.containsKey(received[1])) {
                    removeOperation(received);
                } else {
                    clientWriter.println("ERROR_FILE_DOES_NOT_EXIST");
                    clientWriter.flush();
                    //Handle error
                }
            } else if (received[0].equals("LOAD")) {
                if (!index.containsKey(received[1])) {
                    clientWriter.println("ERROR_FILE_DOES_NOT_EXIST");
                    clientWriter.flush();
                }
                ArrayList<Integer> ports = index.get(received[1]);
                clientWriter.println("LOAD_FROM " + ports.get(1) + " " + ports.get(0));
                clientWriter.flush();
            } else if (received[0].equals("RELOAD")) {
                //TODO
            } else if (received[0].equals("JOIN")) {
                System.out.println(99228);
                //new DStoreHandler(finalClientSocket, this, timeout, null, input).start();
                //TODO
            } else {
                //TODO
            }

        }
    }
}
