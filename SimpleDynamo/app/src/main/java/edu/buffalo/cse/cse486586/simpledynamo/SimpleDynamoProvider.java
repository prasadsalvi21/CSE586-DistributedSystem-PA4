package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final List<String> remotePort = new ArrayList<String>(5);

    static final int SERVER_PORT = 10000;
    private String myPortId;
    private String deviceId;
    private String myPredId;
    private String mySuccId;
    private String myPredHId;
    private String mySuccHId;
    private boolean flag2 = true;
    public static int a = 10;
    public static String q_key = "";
    public static String q_val = "";
    public static Integer failedPort = null;
    public boolean RECOVERY=false;
    private Map<String, String> deviceName = new HashMap<String, String>();
    private TreeMap<String, Node> chord = new TreeMap<String, Node>(new Comparator<String>() {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }

    });
    private ConcurrentHashMap<String, Recover> opeartions = new ConcurrentHashMap<String, Recover>();
    private SimpleDynamoHelper dbHelper;


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        try {
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
            String selection1 = null;
            selection1 = selection;
            String selection2 = SimpleDynamoDB.key + " like '" + selection1 + "'";

            db.execSQL("DELETE FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE " + SimpleDynamoDB.key + " like '" + selection1 + "'");
            opeartions.remove(selection);
            String message = "DELETE," + selection + "," + myPortId;
            //Log.i("Fwd Delete 1 to", mySuccId + " : Message=" + message);
            Caller caller = new Caller(message, Integer.parseInt(mySuccId));
            Thread callerThread = new Thread(caller);
            callerThread.start();

            Integer temp = Integer.parseInt(mySuccId);
            temp = temp / 2;
            String redir = genHash(temp.toString());

            String message2 = "DELETE," + selection + "," + myPortId;
            //Log.i("Fwd Delete 2 to", chord.get(redir).getSucc() + " : Message=" + message2);
            Caller caller2 = new Caller(message2, Integer.parseInt(chord.get(redir).getSucc()));
            Thread callerThread2 = new Thread(caller2);
            callerThread2.start();
            Log.v("Delete Where clause", selection2);
        } catch (NoSuchAlgorithmException e) {
            Log.e("No such algo", Log.getStackTraceString(e));
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    public Uri insert2(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub Insert
        try {

            Log.i(TAG, "For Replica: "+myPortId);
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            String ins = values.toString();
            int index_key = ins.lastIndexOf("key=");
            int index_val = ins.lastIndexOf("value=");
            String key_value = ins.substring(index_key + 4, ins.length());
            String value = ins.substring(index_val + 6, index_key);
            Log.d("Key to be inserted", key_value + ":" + value);

            //Log.i("Inserted for Device", deviceId);
            Cursor c = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key like '" + key_value + "'", null);
            if (c.moveToFirst()) {
                Log.d(TAG, "Key " + key_value + " already present in DB, so update Value corresponding to key");
                update(uri, values, key_value, null);
            } else {
                String Insert_Data = "INSERT INTO " + SimpleDynamoDB.SQLITE_TABLE + " VALUES('" + key_value + "','" + value + "')";
                db.execSQL(Insert_Data);
                getContext().getContentResolver().notifyChange(uri, null);
                Log.v("Inserted values in DB", value + ":" + key_value);
            }

        } catch (Exception e) {
            Log.e("Insert2 exception", "" + e);
        }
        return uri;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub Insert
        try {
            Thread.sleep(947);
            Log.i("RECOVERY STATUS Before:",""+RECOVERY);
            while(RECOVERY) {
            }
            Log.i("RECOVERY STATUS After:",""+RECOVERY);
                SQLiteDatabase db = dbHelper.getWritableDatabase();
                String ins = values.toString();
                int index_key = ins.lastIndexOf("key=");
                int index_val = ins.lastIndexOf("value=");
                String key_value = ins.substring(index_key + 4, ins.length());
                String value = ins.substring(index_val + 6, index_key);
                Log.d("Key to be inserted", key_value + ":" + value);
                String hashedKey = genHash(key_value);
                //Log.d("Hash Key to be inserted", hashedKey);
                //Log.i("My Device", deviceId + ":" + myPortId);
            /*for (Map.Entry<String, Node> entry : chord.entrySet()) {
                if (entry.getValue().getdName().equals(deviceId)) {
                    myPredId = entry.getValue().getPred();
                    mySuccId = entry.getValue().getSucc();
                    myPredHId = entry.getValue().getH_pred();
                    mySuccHId = entry.getValue().getH_succ();
                }
            }*/
                //Log.i("My Device", "DEVICE:" + deviceId + " MYPORTID:" + myPortId + " MYPREDID:" + myPredId + " MYSUCCID:" + mySuccId + " MYHASHPRED:" + myPredHId + " MYHASHSUCC:" + mySuccHId);
                String chordKey = deviceId;
                int checkKey = genHash(chordKey).compareTo(hashedKey);
                int checkPred = myPredHId.compareTo(hashedKey);
                if (checkKey > 0 && checkPred < 0) {

                    Log.i("Inserted for Device", deviceId);
                    Cursor c = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key like '" + key_value + "'", null);
                    if (c.moveToFirst()) {
                        Log.d(TAG, "Key " + key_value + " already present in DB, so update Value corresponding to key");
                        update(uri, values, key_value, null);
                    } else {
                        String Insert_Data = "INSERT INTO " + SimpleDynamoDB.SQLITE_TABLE + " VALUES('" + key_value + "','" + value + "')";
                        db.execSQL(Insert_Data);
                        getContext().getContentResolver().notifyChange(uri, null);
                        Log.v("Inserted values in DB", value + ":" + key_value);
                    }
                    String fromPort = myPortId;
                /*Recover r = new Recover();
                r.setOp("INSERT");
                r.setKey(key_value);
                r.setValue(value);
                r.setMyPortId(fromPort);
                opeartions.put(key_value, r);
                Log.w("Operation added", r.toString());*/
                    //Log.w("Operation", opeartions.toString());
                    //Log.w("operation size", "" + opeartions.size());

                    String message = "REPLICA," + key_value + "," + value + "," + myPortId;
                    Log.i("Fwd Replica 1 to", mySuccId + " : Message=" + message);
                    Caller caller = new Caller(message, Integer.parseInt(mySuccId));
                    Thread callerThread = new Thread(caller);
                    callerThread.start();


                    Integer temp = Integer.parseInt(mySuccId);
                    temp = temp / 2;
                    String redir = genHash(temp.toString());

                    String message2 = "REPLICA," + key_value + "," + value + "," + myPortId;
                    Log.i("Fwd Replica 2 to", chord.get(redir).getSucc() + " : Message=" + message2);
                    Caller caller2 = new Caller(message2, Integer.parseInt(chord.get(redir).getSucc()));
                    Thread callerThread2 = new Thread(caller2);
                    callerThread2.start();
                    callerThread.join();
                    callerThread2.join();
                    boolean rp1_status = caller.sendStatus();
                    boolean rp2_status = caller2.sendStatus();
                    Log.i(TAG, "For Key=" + key_value + " Replica 1 Status=" + rp1_status + " Replica 2 Status=" + rp2_status);
                    if (!rp1_status) {
                        Recover r = new Recover();
                        r.setOp("REPLICA");
                        r.setKey(key_value);
                        r.setValue(value);
                        r.setMyPortId(mySuccId);
                        opeartions.put(key_value, r);
                        Log.w("Operation added", r.toString());

                        String message21 = "FAILURE," + key_value + "," + value + "," + mySuccId + ",REPLICA";
                        Log.i("Fwd Failure Replica 1", chord.get(redir).getSucc() + " : Message=" + message21);
                        Caller caller21 = new Caller(message21, Integer.parseInt(chord.get(redir).getSucc()));
                        Thread callerThread21 = new Thread(caller21);
                        callerThread21.start();
                    } else if (!rp2_status) {
                        Recover r = new Recover();
                        r.setOp("REPLICA");
                        r.setKey(key_value);
                        r.setValue(value);
                        r.setMyPortId(chord.get(redir).getSucc());
                        opeartions.put(key_value, r);
                        Log.w("Operation added", r.toString());

                        String message22 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                        Log.i("Fwd Failure Replica 2 ", mySuccId + " : Message=" + message22);
                        Caller caller22 = new Caller(message22, Integer.parseInt(mySuccId));
                        Thread callerThread22 = new Thread(caller22);
                        callerThread22.start();


                    }

                } else {

                    for (Map.Entry<String, Node> entry1 : chord.entrySet()) {
                        chordKey = entry1.getKey();
                        checkKey = chordKey.compareTo(hashedKey);
                        checkPred = entry1.getValue().getH_pred().compareTo(hashedKey);
                        if (checkKey > 0 && checkPred < 0) {

                            Log.i("Forward Request to devi", entry1.getValue().getdName() + ":" + entry1.getValue().getMyPort());
                            String message = "INSERT," + key_value + "," + value + "," + entry1.getValue().getMyPort();
                            Log.i("Insert send to ", entry1.getValue().getMyPort() + " : Message=" + message);
                            Caller caller = new Caller(message, Integer.parseInt(entry1.getValue().getMyPort()));
                            Thread callerThread = new Thread(caller);
                            callerThread.start();

                            callerThread.join();
                            boolean ins_status = caller.sendStatus();
                            String message3 = "REPLICA," + key_value + "," + value + "," + entry1.getValue().getMyPort();
                            String id = entry1.getValue().getSucc();
                            Log.i("Fwd Replica 1 to", id + " : Message=" + message3);
                            caller = new Caller(message3, Integer.parseInt(id));
                            callerThread = new Thread(caller);
                            callerThread.start();
                            Integer temp = Integer.parseInt(id);
                            temp = temp / 2;
                            String redir = genHash(temp.toString());

                            String message2 = "REPLICA," + key_value + "," + value + "," + entry1.getValue().getMyPort();
                            Log.i("Fwd Replica 2 to", chord.get(redir).getSucc() + " : Message=" + message2);
                            Caller caller2 = new Caller(message2, Integer.parseInt(chord.get(redir).getSucc()));
                            Thread callerThread2 = new Thread(caller2);
                            callerThread2.start();
                            callerThread.join();
                            callerThread2.join();
                            boolean rp1_status = caller.sendStatus();
                            boolean rp2_status = caller2.sendStatus();

                            Log.i(TAG, "For Key" + key_value + " Ins Staus=" + ins_status + " Replica 1 Status=" + rp1_status + " Replica 2 Status=" + rp2_status);
                            if (!ins_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + entry1.getValue().getMyPort() + ",INSERT";
                                Log.i("Fwd Failure 1 to ", id + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(id));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + entry1.getValue().getMyPort() + ",INSERT";
                                Log.i("Fwd Failure 2 to ", chord.get(redir).getSucc() + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(chord.get(redir).getSucc()));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();

                            } else if (!rp1_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + id + ",REPLICA";
                                Log.i("Fwd Failure Ins to ", entry1.getValue().getMyPort() + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(entry1.getValue().getMyPort()));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + id + ",REPLICA";
                                Log.i("Fwd Failue 2 to", chord.get(redir).getSucc() + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(chord.get(redir).getSucc()));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();
                            } else if (!rp2_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                                Log.i("Fwd Failure Ins to ", entry1.getValue().getMyPort() + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(entry1.getValue().getMyPort()));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                                Log.i("Fwd Failure 2 to", id + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(id));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();


                            }

                        } else if (((entry1.getKey().equals(chord.firstKey()) && checkKey > 0) || (entry1.getKey().equals(chord.lastKey()) && checkKey < 0)) && myPortId.equals(chord.get(chord.firstKey()).getMyPort())) {
                            Log.i("Inserted for Device", deviceId);

                            Cursor c = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key like '" + key_value + "'", null);
                            if (c.moveToFirst()) {
                                Log.d(TAG, "Key " + key_value + " already present in DB, so update Value corresponding to key");
                                update(uri, values, key_value, null);
                            } else {
                                String Insert_Data = "INSERT INTO " + SimpleDynamoDB.SQLITE_TABLE + " VALUES('" + key_value + "','" + value + "')";
                                db.execSQL(Insert_Data);
                                getContext().getContentResolver().notifyChange(uri, null);
                                Log.v("Inserted values in DB", value + ":" + key_value);
                            }
                            String fromPort = myPortId;
                        /*Recover r = new Recover();
                        r.setOp("INSERT");
                        r.setKey(key_value);
                        r.setValue(value);
                        r.setMyPortId(fromPort);
                        opeartions.put(key_value, r);
                        Log.w("Operation added", r.toString());*/
                            // Log.w("Operation", opeartions.toString());
                            //Log.w("operation size", "" + opeartions.size());

                            String message = "REPLICA," + key_value + "," + value + "," + myPortId;
                            String id = chord.get(chord.firstKey()).getSucc();
                            Log.i("Fwd Replica 1 to", id + " : Message=" + message);
                            Caller caller = new Caller(message, Integer.parseInt(id));
                            Thread callerThread = new Thread(caller);
                            callerThread.start();


                            Integer temp = Integer.parseInt(id);
                            temp = temp / 2;
                            String redir = genHash(temp.toString());

                            String message2 = "REPLICA," + key_value + "," + value + "," + myPortId;
                            Log.i("Fwd Replica 2 to", chord.get(redir).getSucc() + " : Message=" + message2);
                            Caller caller2 = new Caller(message2, Integer.parseInt(chord.get(redir).getSucc()));
                            Thread callerThread2 = new Thread(caller2);
                            callerThread2.start();
                            callerThread.join();
                            callerThread2.join();
                            boolean rp1_status = caller.sendStatus();
                            boolean rp2_status = caller2.sendStatus();

                            Log.i(TAG, "For Key=" + key_value + " Replica 1 Status=" + rp1_status + " Replica 2 Status=" + rp2_status);
                            if (!rp1_status) {
                                Recover r = new Recover();
                                r.setOp("REPLICA");
                                r.setKey(key_value);
                                r.setValue(value);
                                r.setMyPortId(id);
                                opeartions.put(key_value, r);
                                Log.w("Operation added", r.toString());

                                String message21 = "FAILURE," + key_value + "," + value + "," + id + ",REPLICA";
                                Log.i("Fwd Failure Replica 1", chord.get(redir).getSucc() + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(chord.get(redir).getSucc()));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();
                            } else if (!rp2_status) {
                                Recover r = new Recover();
                                r.setOp("REPLICA");
                                r.setKey(key_value);
                                r.setValue(value);
                                r.setMyPortId(chord.get(redir).getSucc());
                                opeartions.put(key_value, r);
                                Log.w("Operation added", r.toString());

                                String message22 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                                Log.i("Fwd Failure Replica 2 ", mySuccId + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(id));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();


                            }

                        } else if ((entry1.getKey().equals(chord.firstKey()) && checkKey > 0) || (entry1.getKey().equals(chord.lastKey()) && checkKey < 0)) {
                            Log.i("Forward to avd", chord.firstKey() + ":" + chord.get(chord.firstKey()).getMyPort());
                            String message = "INSERT," + key_value + "," + value + "," + chord.get(chord.firstKey()).getMyPort();

                            Log.i("Insert send to ", chord.get(chord.firstKey()).getMyPort() + " : Message=" + message);
                            Caller caller = new Caller(message, Integer.parseInt(chord.get(chord.firstKey()).getMyPort()));
                            Thread callerThread = new Thread(caller);
                            callerThread.start();

                            callerThread.join();
                            boolean ins_status = caller.sendStatus();

                            String message3 = "REPLICA," + key_value + "," + value + "," + chord.get(chord.firstKey()).getMyPort();
                            String id = chord.get(chord.firstKey()).getSucc();
                            Log.i("Fwd Replica 1 to", id + " : Message=" + message3);
                            caller = new Caller(message3, Integer.parseInt(id));
                            callerThread = new Thread(caller);
                            callerThread.start();

                            Integer temp = Integer.parseInt(id);
                            temp = temp / 2;
                            String redir = genHash(temp.toString());

                            String message2 = "REPLICA," + key_value + "," + value + "," + chord.get(chord.firstKey()).getMyPort();
                            Log.i("Fwd Replica 2 to", chord.get(redir).getSucc() + " : Message=" + message2);
                            Caller caller2 = new Caller(message2, Integer.parseInt(chord.get(redir).getSucc()));
                            Thread callerThread2 = new Thread(caller2);
                            callerThread2.start();
                            callerThread.join();
                            callerThread2.join();
                            boolean rp1_status = caller.sendStatus();
                            boolean rp2_status = caller2.sendStatus();

                            Log.i(TAG, "For Key=" + key_value + " Ins Staus=" + ins_status + " Replica 1 Status=" + rp1_status + " Replica 2 Status=" + rp2_status);
                            if (!ins_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + chord.get(chord.firstKey()).getMyPort() + ",INSERT";
                                Log.i("Fwd Failure 1 to ", id + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(id));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + chord.get(chord.firstKey()).getMyPort() + ",INSERT";
                                Log.i("Fwd Failure 2 to ", chord.get(redir).getSucc() + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(chord.get(redir).getSucc()));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();

                            } else if (!rp1_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + id + ",REPLICA";
                                Log.i("Fwd Failure Ins to ", entry1.getValue().getMyPort() + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(chord.get(chord.firstKey()).getMyPort()));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + id + ",REPLICA";
                                Log.i("Fwd Failue 2 to", chord.get(redir).getSucc() + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(chord.get(redir).getSucc()));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();
                            } else if (!rp2_status) {
                                String message21 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                                Log.i("Fwd Failure Ins to ", entry1.getValue().getMyPort() + " : Message=" + message21);
                                Caller caller21 = new Caller(message21, Integer.parseInt(chord.get(chord.firstKey()).getMyPort()));
                                Thread callerThread21 = new Thread(caller21);
                                callerThread21.start();

                                String message22 = "FAILURE," + key_value + "," + value + "," + chord.get(redir).getSucc() + ",REPLICA";
                                Log.i("Fwd Failure 2 to", id + " : Message=" + message22);
                                Caller caller22 = new Caller(message22, Integer.parseInt(id));
                                Thread callerThread22 = new Thread(caller22);
                                callerThread22.start();


                            }
                        }


                    }
                }


        } /*catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException" + e);
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException" + e);
        } */ catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "ClientTask socket Nosuch Algo" + e);
        }
        catch (InterruptedException e)
        {
            Log.e("Intrup I/O",Log.getStackTraceString(e));
        }
        return uri;
    }


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub OnCreate
        try {
            dbHelper = new SimpleDynamoHelper(getContext());
            SQLiteDatabase db = dbHelper.getWritableDatabase();
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            deviceId = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

            myPortId = String.valueOf((Integer.parseInt(deviceId) * 2));

            if (chord.isEmpty())
                createChord();
            Log.i("After onCreate Chord", chord.toString());

            ServerSocket serverSocket = new ServerSocket(); // <-- create an unbound socket first
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(SERVER_PORT)); // <-- now bind it
           //if(serverSocket.) {
               new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
               //Thread.sleep(1);
           /*if(myPortId.equals("11108"))
            Thread.sleep(800);
            else if(myPortId.equals("11112"))
                Thread.sleep(600);
            else if(myPortId.equals("11116"))
                Thread.sleep(400);
            else if(myPortId.equals("11120"))
                Thread.sleep(200);
            else if(myPortId.equals("11124"))
                Thread.sleep(50);*/
            for(int i=0;i<remotePort.size();i++)
            {
                if(!remotePort.get(i).equals(myPortId)) {
                    String message = "RECOVERYSTART," + myPortId;
                    recovery(message, remotePort.get(i));
                }
            }
               new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "a", myPortId);
               while (flag2) {

               }
               flag2 = true;
           //}
                       Log.i(TAG,"RECOVERY completed");

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "Cant create socket" + Log.getStackTraceString(e));
        } /*catch (InterruptedException e) {
            Log.e(TAG, "ClientTask Interrupted exception");
        }*/
        return false;
    }

    public void createChord() {
        try {
            remotePort.add("11108");
            remotePort.add("11112");
            remotePort.add("11116");
            remotePort.add("11120");
            remotePort.add("11124");

            Node n = new Node();
            n.setMyPort("11124");
            n.setdName("5562");
            n.setPred("11120");
            n.setSucc("11112");
            n.setH_pred(genHash("5560"));
            n.setH_succ(genHash("5556"));
            n.setAlive(true);
            chord.put(genHash("5562"), n);
            Node n1 = new Node();
            n1.setMyPort("11112");
            n1.setdName("5556");
            n1.setPred("11124");
            n1.setSucc("11108");
            n1.setH_pred(genHash("5562"));
            n1.setH_succ(genHash("5554"));
            n1.setAlive(true);
            chord.put(genHash("5556"), n1);
            Node n2 = new Node();
            n2.setMyPort("11108");
            n2.setdName("5554");
            n2.setPred("11112");
            n2.setSucc("11116");
            n2.setH_pred(genHash("5556"));
            n2.setH_succ(genHash("5558"));
            n2.setAlive(true);
            chord.put(genHash("5554"), n2);
            Node n3 = new Node();
            n3.setMyPort("11116");
            n3.setdName("5558");
            n3.setPred("11108");
            n3.setSucc("11120");
            n3.setH_pred(genHash("5554"));
            n3.setH_succ(genHash("5560"));
            n3.setAlive(true);
            chord.put(genHash("5558"), n3);
            Node n4 = new Node();
            n4.setMyPort("11120");
            n4.setdName("5560");
            n4.setPred("11116");
            n4.setSucc("11124");
            n4.setH_pred(genHash("5558"));
            n4.setH_succ(genHash("5562"));
            n4.setAlive(true);
            chord.put(genHash("5560"), n4);

            Log.i("My Device", deviceId + ":" + myPortId);
            for (Map.Entry<String, Node> entry : chord.entrySet()) {
                if (entry.getValue().getdName().equals(deviceId)) {
                    myPredId = entry.getValue().getPred();
                    mySuccId = entry.getValue().getSucc();
                    myPredHId = entry.getValue().getH_pred();
                    mySuccHId = entry.getValue().getH_succ();
                }
            }


        } catch (NoSuchAlgorithmException e) {
            Log.e("No such algo chord", "" + e);
        }

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        int seqNumber = 0;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            try {
                ServerSocket serverSocket = sockets[0];
                Socket socket = null;
                String msg, ack;

                while (true) {
                    ack = null;
                    socket = serverSocket.accept();

                    DataInputStream objectInputStream =
                            new DataInputStream(socket.getInputStream());
                    String message = objectInputStream.readUTF();
                    Log.i("Recieved msg client", message);
                    String[] parts = message.split(",");
                    String action = parts[0];


                    if (action.equals("INSERT")) {
                        String key = parts[1];
                        String value = parts[2];
                        String fromPort = parts[3];
                        ContentValues keyValueToInsert = new ContentValues();
                        keyValueToInsert.put("key", key);
                        keyValueToInsert.put("value", value);
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Uri newUri = insert2(uriAddress, keyValueToInsert);
                       // Log.i(TAG, "Sent fwded request");
                        /*Recover r = new Recover();
                        r.setOp("INSERT");
                        r.setKey(key);
                        r.setValue(value);
                        r.setMyPortId(fromPort);
                        opeartions.put(key, r);
                        Log.w("Operation added", r.toString());*/
                        //Log.w("Operation", opeartions.toString());
                        //Log.w("operation size", "" + opeartions.size());


                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    }
                    if (action.equals("REPLICA")) {
                        String key = parts[1];
                        String value = parts[2];
                        String fromPort = parts[3];
                        ContentValues keyValueToInsert = new ContentValues();
                        keyValueToInsert.put("key", key);
                        keyValueToInsert.put("value", value);
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Uri newUri = insert2(uriAddress, keyValueToInsert);
                        //Log.i(TAG, "Sent fwded request");
                       /* Recover r = new Recover();
                        r.setOp("REPLICA");
                        r.setKey(key);
                        r.setValue(value);
                        r.setMyPortId(fromPort);
                        opeartions.put(key, r);
                        Log.w("Operation added", r.toString());*/
                        //Log.w("Operation", opeartions.toString());
                        //Log.w("operation size", "" + opeartions.size());

                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    } else if (action.equals("QUERY")) {
                        String key = parts[1];
                        //String value = parts[2];
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Cursor resultCursor = query2(uriAddress, null,
                                key, null, null);
                        String result1 = "";
                        if (resultCursor.moveToFirst()) {
                            do {
                                String column1 = resultCursor.getString(0);
                                String column2 = resultCursor.getString(1);

                                result1 = result1 + column1 + "," + column2;
                                //Log.i("Appended", column1 + ":" + column2);
                                //Log.i("Query result", result1);


                            } while (resultCursor.moveToNext());
                        }
                        DataOutputStream os4 = new DataOutputStream(socket.getOutputStream());
                        os4.writeUTF("ACK Q OK," + result1);
                        Log.d(TAG, "Sending ACK Q to Server : " + "ACK Q OK," + result1);
                        os4.flush();
                        os4.close();
                    } else if (action.equals("QUERY*")) {
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Cursor resultCursor = query(uriAddress, null,
                                "@", null, null);
                        String result1 = "";
                        if (resultCursor.moveToFirst()) {
                            do {
                                String column1 = resultCursor.getString(0);
                                String column2 = resultCursor.getString(1);

                                result1 = result1 + column1 + ":" + column2 + ";";
                                //Log.i("Appended", column1 + ":" + column2);
                                //Log.i("Query result", result1);


                            } while (resultCursor.moveToNext());
                        }
                        DataOutputStream os4 = new DataOutputStream(socket.getOutputStream());
                        os4.writeUTF("ACK Q OK," + result1);
                        Log.d(TAG, "Sending ACK Q to Server : " + "ACK Q OK," + result1);
                        os4.flush();
                        os4.close();

                    } else if (action.equals("FAILURE")) {
                        //String message21 = "FAILURE," + key_value + "," + value + "," + entry1.getValue().getMyPort()+",INSERT";
                        String failedPort = parts[3];
                        String key_value=parts[1];
                        String value=parts[2];
                        String op=parts[4];
                        Recover r = new Recover();
                        r.setOp(op);
                        r.setKey(key_value);
                        r.setValue(value);
                        r.setMyPortId(failedPort);
                        opeartions.put(key_value, r);
                        Log.w("Operation added", r.toString());

                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();

                        socket.close();
//                        Log.w("Chord after failure", chord.toString());

                    } else if (action.equals("ALIVE")) {
                        String Port = parts[1];
                        Integer tempId = (Integer.parseInt(Port) / 2);
                        String DeviceId = tempId.toString();
                        chord.get(genHash(DeviceId)).setAlive(true);
                        Log.w("Chord after Alive", chord.toString());
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();

                        socket.close();

                    } else if (action.equals("RECOVER")) {
                        String fromPort = parts[1];
                        Integer temp = Integer.parseInt(fromPort) / 2;
                        String fromPred = chord.get(genHash(temp.toString())).pred;
                        String fromSucc = chord.get(genHash(temp.toString())).succ;
                        Integer temp2 = Integer.parseInt(fromPred) / 2;
                        String fromPredPred = chord.get(genHash(temp2.toString())).pred;
                        Log.i(TAG, "FromPort=" + fromPort + " FromPred=" + fromPred + " FromSucc=" + fromSucc + " FromPredPred=" + fromPredPred);
                        String result1 = "";

                        SQLiteDatabase db = dbHelper.getWritableDatabase();
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Log.i("Operations list",opeartions.toString());
                        if (fromPort.equals(myPredId)) {
                            for (Map.Entry<String, Recover> ent : opeartions.entrySet()) {
                                String getPort = ent.getValue().getMyPortId();
                                if ((getPort.equals(fromPort) && ent.getValue().getOp().equals("INSERT")) || (getPort.equals(fromPort) && ent.getValue().getOp().equals("REPLICA"))) {
                                    String column1 = ent.getValue().getKey();
                                    String column2 = ent.getValue().getValue();

                                    result1 = result1 + column1 + ":" + column2 + ";";
                                    //Log.i("Appended", column1 + ":" + column2);
                                   // Log.i("Recover result", result1);

                                }
                            }
                        }
                        if (fromPort.equals(mySuccId)) {
                            for (Map.Entry<String, Recover> ent : opeartions.entrySet()) {
                                String getPort = ent.getValue().getMyPortId();
                                if (getPort.equals(fromPort) && ent.getValue().getOp().equals("REPLICA")) {
                                    String column1 = ent.getValue().getKey();
                                    String column2 = ent.getValue().getValue();

                                    result1 = result1 + column1 + ":" + column2 + ";";
                                    //Log.i("Appended", column1 + ":" + column2);
                                    //Log.i("Recover result", result1);

                                }
                            }
                        }
                        //Log.w("Final recover result",result1);
                        DataOutputStream os4 = new DataOutputStream(socket.getOutputStream());
                        os4.writeUTF("ACK R OK," + result1);
                        Log.d(TAG, "Sending ACK R to Server : " + "ACK R OK," + result1);
                        os4.flush();
                        os4.close();

                    } else if (action.equals("RECOVERYINSERT")) {
                        String key = parts[1];
                        String value = parts[2];
                        // String fromPort=parts[3];
                        ContentValues keyValueToInsert = new ContentValues();
                        keyValueToInsert.put("key", key);
                        keyValueToInsert.put("value", value);
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        Uri newUri = insert2(uriAddress, keyValueToInsert);
                        //Log.i(TAG, "Sent fwded request");

                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    } else if (action.equals("DELETE")) {
                        String key = parts[1];
                        SQLiteDatabase db = dbHelper.getReadableDatabase();
                        Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        //delete(uriAddress,key,null);
                        db.execSQL("DELETE FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE " + SimpleDynamoDB.key + " like '" + key + "'");
                        opeartions.remove(key);
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    }
                    else if(action.equals("REMOVEFAILED"))
                    {
                        String key=parts[1];
                        opeartions.remove(key);
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    }
                    else if(action.equals("RECOVERYSTART"))
                    {
                        String ports=parts[1];
                        RECOVERY=true;
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    }
                    else if(action.equals("RECOVERYEND"))
                    {
                        String ports=parts[1];
                        RECOVERY=false;
                        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                        ack = "server ack";
                        os.writeUTF(ack);
                        os.flush();
                        Log.d(TAG, "Sending ack to Client : " + ack);
                        objectInputStream.close();
                        os.close();
                        socket.close();
                    }
                    objectInputStream.close();
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException" + Log.getStackTraceString(e));
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException" + Log.getStackTraceString(e));
            } catch (NoSuchAlgorithmException e) {
                Log.e("Server task", "" + e);
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {


            return;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public Cursor query2(Uri uri, String[] projection, String selection,
                         String[] selectionArgs, String sortOrder)
    {
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        Cursor cursor1 = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key = '" + selection + "'", null);
    return cursor1;
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        try {
            Thread.sleep(50);
        }
        catch (Exception e)
        {
            Log.e(TAG,"Timeout "+e);
        }
        // TODO Auto-generated method stub

        Log.i("My Device", "DEVICE:" + deviceId + " MYPORTID:" + myPortId + " MYPREDID:" + myPredId + " MYSUCCID:" + mySuccId + " MYHASHPRED:" + myPredHId + " MYHASHSUCC:" + mySuccHId);
        while (RECOVERY)
        {

        }
        if (selection.equals("@")) {
            Log.i("Quering @", selection);
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
            Cursor cursor = null;

            cursor = queryBuilder.query(db, projection, null,
                    selectionArgs, null, null, sortOrder);
            //Log.v("Query Where clause", "@");
            return cursor;
        } else if (selection.equals("*")) {
            Log.i("Quering *", selection);
            try {
                String result = "";
                SQLiteDatabase db = dbHelper.getReadableDatabase();
                SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
                MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                for (Map.Entry<String, Node> ent1 : chord.entrySet()) {
                    String port1 = ent1.getValue().getMyPort();
                    String failedPortS = null;
                    if (failedPort != null)
                        failedPortS = failedPort.toString();
                    Log.i(TAG, "Failed Port: " + failedPortS + "Send Port: " + port1);
                    if (!port1.equals(failedPortS)) {
                        Socket socket5 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(port1));

                        String msgToSend = "QUERY*,";


                        DataOutputStream os5 = new DataOutputStream(socket5.getOutputStream());
                        os5.writeUTF(msgToSend);
                        Log.d(TAG, "Sending Message QUERY* to " + port1 + ": " + msgToSend);
                        os5.flush();

                        DataInputStream is4 = new DataInputStream(new BufferedInputStream(socket5.getInputStream()));
                        Thread.sleep(100);


                        String receivedMsg = is4.readUTF();
                        Log.i("Got query result", receivedMsg);

                        if (receivedMsg.isEmpty())
                            continue;
                        String a[] = receivedMsg.split(",");
                        String r_ack = a[0];
                        String r_res = a[1];

                        String r_fin[] = r_res.split(";");
                       // Log.i("Q* res", r_ack + ":" + r_res + ":" + r_fin);
                        for (int l = 0; l < r_fin.length; l++) {
                            String r_temp[] = r_fin[l].split(":");
                            String r_key = r_temp[0];
                            String r_val = r_temp[1];
                            Log.i("Q* res 1", r_key + ":" + r_val + ":" + r_temp);
                            mc.addRow(new String[]{r_key, r_val});
                        }
                        os5.close();
                        is4.close();
                        socket5.close();
                    }
                }
                //Log.v("Query Where clause1", "*");
                return mc;
            } catch (Exception e) {
                Log.e(TAG, "" + Log.getStackTraceString(e));
            }
        } else {
            try {
                String chordKey = deviceId;
                String hashedKey = genHash(selection);
                int checkKey = genHash(chordKey).compareTo(hashedKey);
                int checkPred = myPredHId.compareTo(hashedKey);
                SQLiteDatabase db = dbHelper.getReadableDatabase();
                SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
                queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
                MatrixCursor mc = new MatrixCursor(new String[]{"key", "value"});
                Cursor cursor1;
                /*Cursor cursor1 = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key = '" + selection + "'", null);
                Log.v("Query Where clause1", selection);
                if (cursor1.getCount() == 1) {
                    return cursor1;
                } else {*/
                    //Thread.sleep(25);
                    for (Map.Entry<String, Node> entry1 : chord.entrySet()) {
                        chordKey = entry1.getKey();
                        checkKey = chordKey.compareTo(hashedKey);
                        checkPred = entry1.getValue().getH_pred().compareTo(hashedKey);
                        if (checkKey > 0 && checkPred < 0) {
                            String id = entry1.getValue().getMyPort();
                            queryReplica(id, selection,mc);

                            //cursor1 = db.rawQuery("SELECT * FROM TEMP_TABLE WHERE key = '" + selection + "'", null);
                            return mc;


                        } else if (((entry1.getKey().equals(chord.firstKey()) && checkKey > 0) || (entry1.getKey().equals(chord.lastKey()) && checkKey < 0)) && myPortId.equals(chord.get(chord.firstKey()).getMyPort())) {
                            cursor1 = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key = '" + selection + "'", null);
                            //Log.v("Query Where clause1", selection);
                            return cursor1;

                        } else if ((entry1.getKey().equals(chord.firstKey()) && checkKey > 0) || (entry1.getKey().equals(chord.lastKey()) && checkKey < 0)) {

                            String id = chord.get(chord.firstKey()).getMyPort();
                            queryReplica(id, selection,mc);

                            //cursor1 = db.rawQuery("SELECT * FROM TEMP_TABLE WHERE key = '" + selection + "'", null);
                            //Log.v("Query Where clause1", selection);
                            return mc;
                        }
                    }
               // }
            } catch (Exception e) {
                Log.e(TAG, "" + Log.getStackTraceString(e));

            }


        }
        return null;
    }

    //synchronized
    private void queryReplica(String id, String selection,MatrixCursor mc) {
        try {

            Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
            SQLiteDatabase db = dbHelper.getReadableDatabase();
            String message = "QUERY," + selection;
            Log.i("Fwd Query to", id + " : Message=" + message);
            QCaller qcaller = new QCaller(message, Integer.parseInt(id));
            Thread qcallerThread = new Thread(qcaller);
            qcallerThread.start();

            qcallerThread.join();
            Log.i("THREAD 1 Q Key=", q_key + ", Q Value:" + q_val);
            String r_key = q_key;
            String r_val = q_val;
            q_val = "";
            q_key = "";


            String port1 = id;
            Integer temp1 = Integer.parseInt(port1) / 2;
            String id1 = chord.get(genHash(temp1.toString())).getSucc();
            String message1 = "QUERY," + selection;
            Log.i("Fwd Query to", id1 + " : Message=" + message1);
            qcaller = new QCaller(message1, Integer.parseInt(id1));
            qcallerThread = new Thread(qcaller);
            qcallerThread.start();

            qcallerThread.join();
            Log.i("THREAD 2 Q Key=", q_key + ", Q Value:" + q_val);
            String r_key1 = q_key;
            String r_val1 = q_val;
            q_val = "";
            q_key = "";

            String port2 = id1;
            Integer temp2 = Integer.parseInt(port2) / 2;
            String id2 = chord.get(genHash(temp2.toString())).getSucc();
            String message2 = "QUERY," + selection;
            Log.i("Fwd Query to", id2 + " : Message=" + message2);
            qcaller = new QCaller(message2, Integer.parseInt(id2));
            qcallerThread = new Thread(qcaller);
            qcallerThread.start();

            qcallerThread.join();
            Log.i("THREAD 3 Q Key=", q_key + ", Q Value:" + q_val);
            String r_key2 = q_key;
            String r_val2 = q_val;
            q_val = "";
            q_key = "";

            Map<String, Integer> m = new HashMap<String, Integer>();
            if (!r_key.isEmpty())
                addMap(m, r_val);
            if (!r_key1.isEmpty())
                addMap(m, r_val1);
            if (!r_key2.isEmpty())
                addMap(m, r_val2);
            //Log.i("MAP Entry", m.toString());

            int maxValueInMap = (Collections.max(m.values()));  // This will return max value in the Hashmap
            String maxValue = r_val;
            for (Map.Entry<String, Integer> entry : m.entrySet()) {  // Itrate through hashmap
                if (entry.getValue() == maxValueInMap) {
                    maxValue = entry.getKey();     // Print the key with max value
                    //String maxValue=entry.getValue();
                }
            }
            String finalKey = "";
            if (r_key.isEmpty() || r_key1.isEmpty())
                finalKey = r_key2;
            else if (r_key.isEmpty() || r_key2.isEmpty())
                finalKey = r_key1;
            else
                finalKey = r_key;

            Log.i("Final max Key=", finalKey + ":" + " for value=" + maxValue);
            mc.addRow(new String[]{finalKey, maxValue});

            //SQLiteDatabase db = dbHelper.getReadableDatabase();
            /*db.execSQL("CREATE TABLE if not exists TEMP_TABLE (key,value);");


            String Insert_Data = "INSERT INTO TEMP_TABLE VALUES('" + finalKey + "','" + maxValue + "')";
            db.execSQL(Insert_Data);
            getContext().getContentResolver().notifyChange(uri, null);*/
        } catch (Exception e) {
            Log.e("Exception", Log.getStackTraceString(e));
        }
    }

    private void addMap(Map<String, Integer> m, String toAdd) {

        Integer currcount = m.get(toAdd);
        if (currcount == null)
            m.put(toAdd, 1);
        else
            m.put(toAdd, currcount + 1);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
        queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
        String selection2 = SimpleDynamoDB.key + " like '" + selection + "'";
        //Log.i("Selection",selection2);
        String msg = values.getAsString(SimpleDynamoDB.value);
        values.put(SimpleDynamoDB.value, msg);
        Log.v("DB updated with value", msg);
        db.update(SimpleDynamoDB.SQLITE_TABLE, values, selection2, null);
        return 0;
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    synchronized private void recovery(String message, String myportId) {
        //String message = "RECOVERYINSERT," + r_key + "," + r_val + "," + myPortId;
        //Log.i("Insert send to ", myportId + " : Message=" + message);
        Caller caller = new Caller(message, Integer.parseInt(myportId));
        Thread callerThread = new Thread(caller);
        callerThread.start();

    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            //String a[],r_ack,r_res,r_fin[];
            final SQLiteDatabase db = dbHelper.getReadableDatabase();
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(SimpleDynamoDB.SQLITE_TABLE);
            Uri uriAddress = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
            db.execSQL("CREATE TABLE if not exists TEMP_TABLE2 (key,value);");
            try {
                RECOVERY=true;

                final String fromPort = myPortId;
                Integer temp = Integer.parseInt(fromPort) / 2;
                final String fromPred = chord.get(genHash(temp.toString())).pred;
                final String fromSucc = chord.get(genHash(temp.toString())).succ;
                Integer temp2 = Integer.parseInt(fromPred) / 2;
                final String fromPredPred = chord.get(genHash(temp2.toString())).pred;
                Log.i(TAG, "FromPort=" + fromPort + " FromPred=" + fromPred + " FromSucc=" + fromSucc + " FromPredPred=" + fromPredPred);
                // for (String rp : remotePort) {

                //Log.i("RemotePort",rp);

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(fromPred));

                //String msgToSend = msgs[0];
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                String ack = null;
                String b[] = null;
                do {
                    ack = null;
                    DataOutputStream os = new DataOutputStream(socket.getOutputStream());
                    os.writeUTF("RECOVER," + myPortId);
                    Log.d(TAG, "Sending recovery to " + fromPred + "  : " + "RECOVER," + myPortId);
                    os.flush();
                    //Thread.sleep(500);
                    DataInputStream is = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                    ack = is.readUTF();
                    b = ack.split(",");
                    //Log.d(TAG, "Receiving ack from Server : " + ack);
                } while (!b[0].equals("ACK R OK"));
                socket.close();
                Log.i("Got query result from =", fromPred + ":" + ack);
                if (ack.length() > 10) {
                    String a[] = ack.split(",");
                    String r_ack = a[0];
                    String r_res = a[1];

                    String r_fin[] = r_res.split(";");
                    Log.i("Q* res", r_ack + ":" + r_res + ":" + r_fin);


                    for (int l = 0; l < r_fin.length; l++) {
                        String r_temp[] = r_fin[l].split(":");
                        String r_key = r_temp[0];
                        String r_val = r_temp[1];
                        // String r_port=r_temp[2];

                        Log.i("Recovery result", r_key + ":" + r_val + ":" + genHash(r_key));
                        Cursor c = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key like '" + r_key + "'", null);
                        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        if (c.moveToFirst()) {
                            Log.d(TAG, "Key " + r_key + " already present in DB, so update Value corresponding to key");
                            String Update_Data = "UPDATE " + SimpleDynamoDB.SQLITE_TABLE + " SET key='" + r_key + "',value='" + r_val + "' WHERE key like '" + r_key + "'";
                            db.execSQL(Update_Data);
                            getContext().getContentResolver().notifyChange(uri, null);
                            Log.v("Updated values in DB", r_val + ":" + r_key);
                        } else {
                            String Insert_Data = "INSERT INTO " + SimpleDynamoDB.SQLITE_TABLE + " VALUES('" + r_key + "','" + r_val + "')";
                            db.execSQL(Insert_Data);
                            getContext().getContentResolver().notifyChange(uri, null);
                            Log.v("Inserted values in DB", r_val + ":" + r_key);
                        }


                      /* String message = "RECOVERYINSERT," + r_key + "," + r_val + "," + myPortId;
                        recovery(message, myPortId);*/

                                    /*Recover r = new Recover();
                                    r.setOp("REPLICA");
                                    r.setKey(r_key);
                                    r.setValue(r_val);
                                    r.setMyPortId(myPortId);
                                    opeartions.put(r_key, r);
                                    Log.w("Operation added", r.toString());*/
                        String message = "REMOVEFAILED," + r_key + "," + r_val + "," + myPortId;
                        recovery(message, fromPred);
                        recovery(message, fromPredPred);

                    }

                }

                Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(fromSucc));

                //String msgToSend1 = msgs[0];
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */
                String ack1 = null;
                String b1[] = null;
                do {
                    ack1 = null;
                    DataOutputStream os = new DataOutputStream(socket1.getOutputStream());
                    os.writeUTF("RECOVER," + myPortId);
                    Log.d(TAG, "Sending recovery to " + fromSucc + "  : " + "RECOVER," + myPortId);
                    os.flush();
                    //Thread.sleep(500);
                    DataInputStream is = new DataInputStream(new BufferedInputStream(socket1.getInputStream()));
                    ack1 = is.readUTF();
                    b1 = ack1.split(",");
                    //Log.d(TAG, "Receiving ack from Server : " + ack);
                } while (!b1[0].equals("ACK R OK"));
                socket1.close();
                Log.i("Got query result from =", fromSucc + ":" + ack1);
                if (ack1.length() > 10) {
                    String a1[] = ack1.split(",");
                    String r_ack1 = a1[0];
                    String r_res1 = a1[1];

                    String r_fin1[] = r_res1.split(";");
                    Log.i("Q* res", r_ack1 + ":" + r_res1 + ":" + r_fin1);


                    for (int l = 0; l < r_fin1.length; l++) {
                        String r_temp1[] = r_fin1[l].split(":");
                        String r_key1 = r_temp1[0];
                        String r_val1 = r_temp1[1];
                        // String r_port=r_temp[2];
                        Log.i("Recovery result", r_key1 + ":" + r_val1 + ":" + genHash(r_key1));

                       /* String message = "RECOVERYINSERT," + r_key1 + "," + r_val1 + "," + myPortId;
                        recovery(message, myPortId);*/

                        Cursor c = db.rawQuery("SELECT * FROM " + SimpleDynamoDB.SQLITE_TABLE + " WHERE key like '" + r_key1 + "'", null);
                        Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
                        if (c.moveToFirst()) {
                            Log.d(TAG, "Key " + r_key1 + " already present in DB, so update Value corresponding to key");
                            String Update_Data = "UPDATE "+SimpleDynamoDB.SQLITE_TABLE+" SET key='" + r_key1 + "',value='" + r_val1 +"' WHERE key like'"+r_key1+"'";
                            db.execSQL(Update_Data);
                            getContext().getContentResolver().notifyChange(uri, null);
                            Log.v("Updated values in DB", r_val1 + ":" + r_key1);
                        } else {
                            String Insert_Data = "INSERT INTO " + SimpleDynamoDB.SQLITE_TABLE + " VALUES('" + r_key1 + "','" + r_val1 + "')";
                            db.execSQL(Insert_Data);
                            getContext().getContentResolver().notifyChange(uri, null);
                            Log.v("Inserted values in DB", r_val1 + ":" + r_key1);
                        }

/*                        Recover r = new Recover();
                        r.setOp("INSERT");
                        r.setKey(r_key1);
                        r.setValue(r_val1);
                        r.setMyPortId(myPortId);
                        opeartions.put(r_key1, r);
                        Log.w("Operation added", r.toString());*/

                        String message = "REMOVEFAILED," + r_key1 + "," + r_val1 + "," + myPortId;
                        recovery(message, fromSucc);

                    }

                }

                flag2 = false;
                RECOVERY=false;
                for(int i=0;i<remotePort.size();i++)
                {
                    if(!remotePort.get(i).equals(myPortId)) {
                        String message = "RECOVERYEND," + myPortId;
                        recovery(message, remotePort.get(i));
                    }
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException" + Log.getStackTraceString(e));
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException " + myPortId + "" + Log.getStackTraceString(e));
                flag2 = false;
                RECOVERY=false;
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "No such algor" + Log.getStackTraceString(e));
            }
            return null;
            /*catch (InterruptedException e) {
                        Log.e(TAG, "ClientTask socket Intrup"+ Log.getStackTraceString(e));
                    }*/

        }

    }
}


