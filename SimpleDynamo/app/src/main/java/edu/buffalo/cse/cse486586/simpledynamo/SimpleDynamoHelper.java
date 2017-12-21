package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by prasad-pc on 2/17/17.
 */
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class SimpleDynamoHelper extends SQLiteOpenHelper
{

    private static final String DATABASE_NAME = "PA4";
    private static final int DATABASE_VERSION = 1;

    SimpleDynamoHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        SimpleDynamoDB.onCreate(db);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        SimpleDynamoDB.onUpgrade(db, oldVersion, newVersion);
    }

}
