import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Stack;

public class DStore implements Runnable {
    //TODO if folders already exist add files to stack
    private ArrayList<File> files;
    private final int port;
    private final int cPort;
    private long timeout;
    private final String fileFolder;
    private ServerSocket ss;
    private Stack<File> arrayDeque;

    public DStore(int port, int cPort, long timeout, String fileFolder) {
        this.port = port;
        this.cPort = cPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.files = new ArrayList<>();
        this.arrayDeque = new Stack<>();
        try {
            this.ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ArrayList<File> getFiles() {
        return files;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        int cPort = Integer.parseInt(args[1]);
        long timeout = Long.parseLong(args[2]);
        String fileFolder = args[3];

        File file = new File(fileFolder);
        if (!file.exists()) {
            if (file.mkdir()) {
                System.out.println("Directory " + fileFolder + " has been created.");
            } else {
                System.out.println("This directory already exists.");
            }
        }

        //Creating the Controller
        DStore dStore = new DStore(port, cPort, timeout, fileFolder);
        dStore.run();
    }

    @Override
    public void run() {
        try {
            Socket controllerSocket = new Socket();
            controllerSocket.connect(new InetSocketAddress(cPort));
            new Thread(() -> {
                while (true) {
                    try {
                        OutputStream out = controllerSocket.getOutputStream();
                        PrintWriter printWriter = new PrintWriter(out);
                        printWriter.println(port);
                        printWriter.flush();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                        String received;
                        if ((received = bufferedReader.readLine()) != null) {
                            if (received.contains("aa")) {


                                out.close();
                            }
                            //TODO store_ack
                        }

                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }).start();


            Socket clientSocket = ss.accept();
            new Thread(() -> {
                BufferedReader in1 = null;
                PrintWriter out1 = null;
                try {
                    in1 = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
                    out1 = new PrintWriter(clientSocket.getOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String input;
                while (true) {
                    try {
                        if ((input = in1.readLine()) != null) {
                            if(input.contains("STORE")){
                                String[] temp = input.split(" ");
                                String fileName = temp[1];
                                long fileSize = Long.parseLong(temp[2]);
                                File file = new File(fileFolder, fileName); //TODO check
                                if (!file.exists()) {
                                    if (file.createNewFile()) {
                                        files.add(file);
                                        System.out.println("The file " + fileName + " has been created.");
                                    } else {
                                        System.out.println("The file already exists.");
                                    }
                                }
                                System.out.println(56);
                                out1.println("ACK");
                                out1.flush();
                                input = in1.readLine();
                                FileWriter fileWriter = new FileWriter(file);
                                fileWriter.write(input);
                                fileWriter.flush();
                                out1.println("STORE_ACK " + file.getName());
                                out1.flush();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}