import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class Dstore implements Runnable {
    //TODO if folders already exist add files to stack
    private HashMap<String, Long> files;
    private final int port;
    //  private final int cPort;
    private long timeout;
    private final String fileFolder;
    private final PrintWriter controllerWriter;
    private final BufferedReader controllerReader;
    private ServerSocket ss;


    public Dstore(int port, int cPort, long timeout, String fileFolder) {
        System.out.println("Dstore creation");
        this.port = port;
        //  this.cPort = cPort;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
        this.files = new HashMap<>();
        System.out.println("Dstore creation1");
        try {
            Socket controllerSocket = new Socket(InetAddress.getLoopbackAddress(), cPort);
            OutputStream out = controllerSocket.getOutputStream();
            controllerWriter = new PrintWriter(out);
            controllerReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        } catch (IOException e) {
            System.out.println("Error9");
            throw new RuntimeException(e);
        }
        System.out.println("Dstore creation2");
        controllerWriter.println("JOIN " + port);
        controllerWriter.flush();
        System.out.println("Dstore creation3");
        try {
            this.ss = new ServerSocket();
            ss.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
        } catch (IOException e) {
            System.out.println("Error8");
            e.printStackTrace();
        }
        System.out.println("Dstore creation4");
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
            }
        } else {
            System.out.println("This directory already exists.");
            for (File current : Objects.requireNonNull(file.listFiles())) {
                current.delete();
            }
        }

        //Creating the Controller
        Dstore dStore = new Dstore(port, cPort, timeout, fileFolder);
        dStore.run();
    }

    @Override
    public void run() {
        System.out.println("DSTORE START");
        //Connection with controller
        new Thread(() -> {
            System.out.println("Initialising communication with controller.");
            try {
                while (true) {
                    String received = controllerReader.readLine();
                    if (received != null) {
                        String[] input = received.split(" ");
                        switch (input[0]) {
                            case "REMOVE":
                                removeOperation(input, controllerWriter);
                                break;
                            case "LIST":
                                controllerWriter.println("LIST " + getFileNames());
                                controllerWriter.flush();
                                break;
                            case "REBALANCE":
                                synchronized (this) {
                                    System.out.println("Starting rebalance: " + Arrays.toString(input));
                                    int NFilesToStore = Integer.parseInt(input[1]);
                                    rebalanceStore(NFilesToStore, input);
                                    rebalanceRemove(NFilesToStore, input);
                                }
                                break;
                            case "ACK":
                                System.out.println(24);
                                break;
                            default:
                                System.out.println(received);
                                break;
                        }
                    }
                }

            } catch (IOException e1) {
                System.out.println("Error7");
                e1.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            System.out.println("Initializing handler.");
            while (true) {
                try {
                    Socket currentSocket = ss.accept();
                    new ClientDStoreHandler(currentSocket, this).start();
                } catch (IOException e1) {
                    System.out.println("Error6");
                    e1.printStackTrace();
                }
            }

        }).start();
    }

    public void receiveMessages(String input, ClientDStoreHandler clientDStoreHandler) {
        try {
            PrintWriter printWriter = clientDStoreHandler.getdOut();
            OutputStream outputStream = clientDStoreHandler.getOutputStream();
            InputStream inputStream = clientDStoreHandler.getInputStream();

            System.out.println("Receiving a new message..");

            String[] inputSplit = input.split(" ");
            switch (inputSplit[0]) {
                case "STORE":
                    storeOperation(inputSplit, printWriter, controllerWriter, inputStream);
                    break;
                case "LOAD_DATA":
                    String filename = inputSplit[1];

                    if (files.containsKey(filename)) {
                        FileInputStream in = new FileInputStream(fileFolder + "/" + filename);
                        byte[] buffer = new byte[Math.toIntExact(files.get(filename))];
                        int len;
                        while ((len = in.read(buffer)) > 0) {
                            outputStream.write(buffer, 0, len);
                            outputStream.flush();

                        }
                        in.close();
                    } else {
                        clientDStoreHandler.getdStoreSocket().close();
                    }
                    break;
                case "REBALANCE_STORE":
                    synchronized (this) {
                        System.out.println(33);
                        String fileName = inputSplit[1];
                        Long fileSize = Long.parseLong(inputSplit[2]); //TODO why is null received
                        File file = new File(fileFolder, fileName);
                        storeFile(fileName, fileSize, file);
                        // files.put(fileName, fileSize);
                        printWriter.println("ACK");
                        printWriter.flush(); //Doesn't go where I want it
                    }
                    break;
                case "ACK":
                    System.out.println(77);
                    break;
                default:
                    System.out.println(input);
                    //Ignore and log
                    break;
            }

        } catch (IOException e) {
            System.out.println("Error5");
            e.printStackTrace();
        }
    }

    private boolean storeFileContent(File file, byte[] data) {
        try {
            OutputStream os = new FileOutputStream(file);
            os.write(data);
            os.flush();
            os.close();
            System.out.println("File stored successfully.");
            return true;
        } catch (IOException e) {
            System.out.println("Error4");
            e.printStackTrace();
            return false;
        }
    }


    private void storeOperation(String[] input, PrintWriter finalOut, PrintWriter printWriter,
                                InputStream inputReader) {
        System.out.println("STORE operation starts.");
//        try {
        String fileName = input[1];
        long fileSize = Long.parseLong(input[2]);
        File file = new File(fileFolder, fileName);
        storeFile(fileName, fileSize, file);

        int size = (int) fileSize;
        byte[] data = new byte[size];
        //   System.out.println("BEFORE");


        finalOut.println("ACK");
        finalOut.flush();
        System.out.println("Store ACK sent to Client.");

        try {
            inputReader.readNBytes(data, 0, size);
            System.out.println("AFTER");
            if (storeFileContent(file, data)) {
                printWriter.println("STORE_ACK " + file.getName());
                printWriter.flush();
                System.out.println("STORE_ACK sent to Controller.");
            } else {
                //TODO
            }
        } catch (IOException e) {
            System.out.println("Error2");
            e.printStackTrace();
        }


    }

    private void storeFile(String fileName, Long fileSize, File file) {
        try {
            if (!file.exists()) {
                if (file.createNewFile()) {
                    System.out.println(fileSize);
                    files.put(fileName, fileSize);
                    System.out.println("The file " + fileName + " has been created.");
                } else {
                    System.out.println("The file already exists.");
                }
            }
        } catch (IOException e) {
            System.out.println("Error3");
            e.printStackTrace();
        }
    }

    private void rebalanceStore(int NFilesToStore, String[] input) throws IOException {

        for (int i = 2; i < NFilesToStore * 3 + 2; i += 3) {
            String fileName = input[i];
            int storage = Integer.parseInt(input[i + 2]);
            Socket receiver = new Socket(InetAddress.getLoopbackAddress(), storage);
            OutputStream outStr = receiver.getOutputStream();
            PrintWriter printer = new PrintWriter(outStr);

            System.out.println("Storing " + fileName + " to DStore " + storage);

            printer.println("REBALANCE_STORE " + fileName + " " + files.get(fileName));
            printer.flush();

            receiver.close();

        }
    }

    private void rebalanceRemove(int NFilesToStore, String[] input) throws IOException {
        for (int i = NFilesToStore * 3 + 3; i < input.length; i++) {
            String fileToRemove = input[i];
            Path filePath = Path.of(fileFolder + "/" + fileToRemove);
            Files.delete(filePath);
            files.remove(fileToRemove);
            System.out.println("Removing " + fileToRemove + " from DStore " + port);
            //Update index
        }
    }

    private void removeOperation(String[] input, PrintWriter printWriter) {
        try {
            String fileName = input[1];
            if (files.containsKey(fileName)) {
                Path filePath = Path.of(fileFolder + "/" + fileName);
                Files.delete(filePath);
                files.remove(fileName);
                printWriter.println("REMOVE_ACK " + fileName);
                printWriter.flush();
            } else {
                printWriter.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
                printWriter.flush();
            }
        } catch (IOException e) {
            System.out.println("Error10");
            e.printStackTrace();
        }
    }

    private String getFileNames() {
        StringBuilder sb = new StringBuilder();
        for (String name : files.keySet()) {
            sb.append(name).append(" ");
        }
        return sb.toString();
    }

}