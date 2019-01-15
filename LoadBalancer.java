import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.lang.Short;
import java.util.HashMap;

public class LoadBalancer {

    public static void main(String[] args) throws IOException {

        final String GETOCC = "GETOCC:\n"; 

        String routerIp = args[0].split(":")[0];
        int routerPort = Integer.parseInt(args[0].split(":")[1]);
        InetAddress addr = InetAddress.getByName(routerIp);
        HashMap<Integer, String> opMap = new HashMap<Integer, String>();
        String key = "";

        for(int i = 1; i < args.length; i++) {
                opMap.put(i, args[i]);
        }

        ServerSocket routerSocket = new ServerSocket(routerPort, 0, addr);
        Socket router = routerSocket.accept();
        DataOutputStream outToClient = new DataOutputStream(router.getOutputStream());
        DataInputStream inFromClient = new DataInputStream(new BufferedInputStream(router.getInputStream()));

        Socket opSocket;
		DataOutputStream outToOp;
		DataInputStream inFromOp;
        String incoming = "";
        String opIp = "";
        int opPort;
        String data = "";
        byte[] dataBytes = null;
        String[] ops = null;
        HashMap<String, String[]> dataFuncMap = new HashMap<String, String[]>();
        HashMap<Integer, Integer> occMap = new HashMap<Integer, Integer>();
        HashMap<Integer, String> dataMap = new HashMap<Integer, String>();
        HashMap<Integer, Socket> socketMap = new HashMap<Integer, Socket>();
        HashMap<Integer, DataInputStream> inputStreamMap = new HashMap<Integer, DataInputStream>();
        HashMap<Integer, DataOutputStream> outputStreamMap = new HashMap<Integer, DataOutputStream>();

        int count = 0;
        while (count != 2) {
            incoming = inFromClient.readLine();
            dataFuncMap.put(incoming.split(":")[0], incoming.split(":")[1].split(","));
            count ++;
        }
        
        for(int i = 1; i <= opMap.size(); i++) {
            opIp = opMap.get(i).split(":")[0];
            opPort = Integer.parseInt(opMap.get(i).split(":")[1]);
            opSocket = new Socket(InetAddress.getByName(opIp), opPort);
            socketMap.put(i, opSocket);
            inFromOp = new DataInputStream(new BufferedInputStream(opSocket.getInputStream()));
            inputStreamMap.put(i, inFromOp);
            outToOp = new DataOutputStream(opSocket.getOutputStream());
            outputStreamMap.put(i, outToOp);
            outToOp.write(GETOCC.getBytes("UTF-8"));
            data = inFromOp.readLine();
            occMap.put(i, Integer.parseInt(data.split(":")[1]));
        }

        int dataSize = dataFuncMap.get("DATA").length;
        int noOfOp = opMap.size();
        int procElements = 0;
        int start = 0;
        int end = 0;
        float totalOcc = 0;

        for(int i = 1; i <= occMap.size(); i++) {
            totalOcc += (float)(1.0 / (occMap.get(i) + 1));
        }
        String[] dataArray = dataFuncMap.get("DATA");

        for(int i = 1; i <= occMap.size(); i++) {
            float p = (float)(1.0 / (occMap.get(i) + 1));
            p = p / totalOcc;
            int noOfElements = (int)(p * dataSize);
            procElements += noOfElements;
            if(i == opMap.size())
                noOfElements += dataSize - procElements;
            end = noOfElements;
            data = Arrays.toString(Arrays.copyOfRange(dataArray, 0, end));
            data = data.substring(1, data.length() - 1);
            dataMap.put(i, data);
            dataArray = Arrays.copyOfRange(dataArray, end, dataArray.length);
        }
        String[] funcArray = dataFuncMap.get("FUNCS");
        String dataOut = "";
        String dataIn = "";
        String temp = "";

        for(int i = 0; i < funcArray.length; i++) {
            for(int a = 1; a <= opMap.size(); a++) {
                dataOut = "DATA:" + dataMap.get(a) + "\n";
                outToOp = outputStreamMap.get(a);
                outToOp.write(dataOut.getBytes("UTF-8"));
                dataOut = "FUNCS:" + funcArray[i] + "\n";
                outToOp.write(dataOut.getBytes("UTF-8"));
                inFromOp = inputStreamMap.get(a);
                dataIn = inFromOp.readLine();
                dataMap.put(a, dataIn.split(":")[1]);
            }
        }

        for(int i = 1; i <= opMap.size(); i++) {
            dataOut = "END:\n";
            outToOp = outputStreamMap.get(i);
            outToOp.write(dataOut.getBytes("UTF-8"));
            socketMap.get(i).close();

        }

        dataOut = "DATA:";
        for(int i = 1; i <= dataMap.size(); i++) {
            dataOut += dataMap.get(i);
            if(i != dataMap.size())
                dataOut += ",";
        }
        
        dataOut += "\n";
        outToClient.write(dataOut.getBytes("UTF-8"));
        outToClient.close();
        inFromClient.close();
        router.close();
    
   }
}
