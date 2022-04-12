import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class DStore implements Runnable {
    //TODO if folders already exist add files to stack
    private HashMap<String, Long> files;
    private final int port;
    private final int cPort;
    private long timeout;
    private final String fileFolder;
    private ServerSocket ss;

    public DStore(int port, int cPort, long timeout, String fileFolder) {
        this.port = port;
        this.cPort = cPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.files = new HashMap<>();
        try {
            this.ss = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HashMap<String, Long> getFiles() {
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

        //Connection with controller
        try {
            Socket controllerSocket = new Socket();
            controllerSocket.connect(new InetSocketAddress(cPort));
            OutputStream out = controllerSocket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(out);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            printWriter.println("JOIN " + port);
            printWriter.flush();
            new Thread(() -> {
                try {
                    while (true) {
                        String received = bufferedReader.readLine();
                        if (received != null) {
                            String[] input = received.split(" ");
                            if (input[0].equals("REMOVE")) {
                                String fileName = input[1];
                                System.out.println(88);
                                if (files.containsKey(fileName)) {
                                    System.out.println(77);
                                    printWriter.println("REMOVE_ACK " + fileName);
                                    printWriter.flush();
                                    Path filePath = Path.of(fileFolder + "/" + fileName);
                                    Files.delete(filePath);
                                    files.remove(fileName);
                                } else {
                                    printWriter.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
                                    printWriter.flush();
                                }
                            } else {
                                //Ignore and log
                            }
                        }
                    }

                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }).start();


            //Communication with client
            new Thread(() -> {
                String input;
                while (true) {
                    Socket clientSocket = null;
                    try {
                        clientSocket = ss.accept();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    BufferedReader in1 = null;
                    PrintWriter out1 = null;
                    try {
                        in1 = new BufferedReader(
                                new InputStreamReader(clientSocket.getInputStream()));
                        out1 = new PrintWriter(clientSocket.getOutputStream());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    BufferedReader finalIn = in1;
                    PrintWriter finalOut = out1;
                    try {
                        input = finalIn.readLine();
                        if (input != null) {
                            String[] inputSplit = input.split(" ");
                            if (inputSplit[0].equals("STORE")) {
                                String[] temp = input.split(" ");
                                String fileName = temp[1];
                                long fileSize = Long.parseLong(temp[2]);
                                File file = new File(fileFolder, fileName); //TODO check
                                if (!file.exists()) {
                                    if (file.createNewFile()) {
                                        files.put(fileName, fileSize);
                                        System.out.println("The file " + fileName + " has been created.");
                                    } else {
                                        System.out.println("The file already exists.");
                                    }
                                }
                                finalOut.println("ACK");
                                finalOut.flush();
                                input = finalIn.readLine();
                                FileWriter fileWriter = new FileWriter(file);
                                fileWriter.write(input);
                                fileWriter.flush();
                                printWriter.println("STORE_ACK " + file.getName());
                                printWriter.flush();
                            } else if (inputSplit[0].equals("LOAD_DATA")) {
                                String filename = inputSplit[1];
                                if (files.containsKey(filename)) {
                                    Path filePath = Path.of(fileFolder + "/" + filename);
                                    String content = Files.readString(filePath);
                                    finalOut.println(content);
                                    finalOut.flush();
                                } else {
                                    //Handle error
                                }
                            } else {
                                //Ignore and log
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }

}