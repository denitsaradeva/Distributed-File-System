import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Controller implements Runnable {

    //index map -> file <-> port, size
    // TODO : message - ex: processing ?
    private HashMap<String, ArrayList<Integer>> index;

    //The set of all current DStores
    private HashSet<Socket> dStores;
    private HashSet<Integer> ports;

    //The socket responsible for the controller's communication
    private final ServerSocket ss;
    //  private Socket socket;

    //Mandatory fields
    int cPort;
    int R;
    long timeout;
    long rebalancePeriod;

    public Controller(int cPort, int R, long timeout, long rebalancePeriod) throws IOException {
        this.dStores = new HashSet<>();
        this.ports = new HashSet<>();
        this.index = new HashMap<>();
        this.cPort = cPort;
        this.R = R;
        this.timeout = timeout;
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

        Socket client;
        Socket dStore;

        //Connecting the DStores
        try {
            for (int i = 0; i < R; i++) {
                dStore = ss.accept();
                BufferedReader dIn = new BufferedReader(new InputStreamReader(dStore.getInputStream()));
                String received;
                received = dIn.readLine();
                if (received != null) {
                    int port = Integer.parseInt(received);
                    System.out.println("DStore with port " + received + " is connected.");
                    ports.add(port);
                }
                Socket finalDStore = dStore;
                new Thread(() -> {
                    try {
                        PrintWriter dOut = new PrintWriter(finalDStore.getOutputStream());
                        String input;
                        while (true) {
                            if ((input = dIn.readLine()) != null) {
                                if (input.contains("STORE_ACK")) {
                                    System.out.println(999);
                                    dOut.write("STORE_COMPLETE");
                                    dOut.flush();
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).start();
            }
            System.out.println("All dStores are now connected.");

            //Connecting the client
            client = ss.accept();
            new Thread(() -> {
                BufferedReader in = null;
                PrintWriter out = null;
                try {
                    in = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                    out = new PrintWriter(client.getOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String received;
                while (true) {
                    try {
                        Thread.sleep(500);
                        received = in.readLine();
                        if (received != null) {
                            if (received.contains("LIST")) {
                                System.out.println(88);
                                out.println("LIST");
                                out.flush();
                            } else if (received.contains("STORE")) {
                                String[] input = received.split(" ");
                                System.out.println(99);
                                out.println("STORE_TO " + getPortsAsString()); // BIG TODO
                                out.flush();
                                index.put(input[1], new ArrayList<>(getPorts()));
                            } else if (received.contains("REMOVE")) {

                            } else {
                                System.out.println(1000);
                                System.out.println(received);
                            }

                        }
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getPortsAsString() {
        String result = "";
        for (Integer port : getPorts()) {
            result += port + " ";
        }
        return result;
    }

    public HashSet<Integer> getPorts() {
        return ports;
    }
}
