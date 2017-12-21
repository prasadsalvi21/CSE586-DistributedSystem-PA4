package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by prasad-pc on 4/30/17.
 */

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

import android.util.Log;

public class Caller implements Runnable {
    private String message;
    private int destinationPort;
    private boolean sendSuccessful = false;
    private int ports[] = {11108, 11112, 11116, 11120, 11124};
    private boolean flag=false;

    public Caller(String message, int destinationPort) {
        this.message = message;
        this.destinationPort = destinationPort;
    }

    public void run() {
        try {
            InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
            Socket socket3 = new Socket(address, destinationPort);
            String ack = "";
            do {

                ack = null;


                DataOutputStream os3 = new DataOutputStream(socket3.getOutputStream());
                os3.writeUTF(message);
                Log.d("Sending Insert to Ser:", destinationPort+":"+message);
                os3.flush();
                //Thread.sleep(100);
                DataInputStream is = new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                ack = is.readUTF();
                Log.d("Receiving ack from Ser:",destinationPort+":"+ ack);
                //SimpleDynamoProvider.a=100;
            } while(!ack.equals("server ack")) ;
            if(ack.equals("server ack"))
                flag=true;
            socket3.close();
        } /*catch (InterruptedException e) {
            e.printStackTrace();
        }*/ catch (IOException e) {
            Log.e("Sender Exception", "IOException in Sender " + e);
            SimpleDynamoProvider.failedPort=destinationPort;
            /*try {
                for (int i = 0; i < 4; i++) {
                    if (ports[i] != destinationPort) {
                        InetAddress address = InetAddress.getByAddress(new byte[]{10, 0, 2, 2});
                        Socket socket3 = new Socket(address, ports[i]);
                        String msg="FAILURE,"+destinationPort;
                        DataOutputStream os3 = new DataOutputStream(socket3.getOutputStream());
                        os3.writeUTF(message);
                        Log.d("Sending Insert to Serv:", message);
                        os3.flush();
                        Thread.sleep(100);
                        DataInputStream is = new DataInputStream(new BufferedInputStream(socket3.getInputStream()));
                        String ack = is.readUTF();
                        Log.d("Receiving ack from:", ack);
                        if (!ack.equals("server ack")) ;
                        socket3.close();
                    }
                }

            } catch (Exception e1) {
                Log.e("Inform failure",""+e1);
            }*/
        }
    }
    public boolean sendStatus(){
        return flag;
    }


   }