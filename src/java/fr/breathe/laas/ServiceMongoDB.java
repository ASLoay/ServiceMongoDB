/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package fr.breathe.laas;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;

/**
 *
 * @author loay
 */
/**
     * Web service getting a query
*/
@WebService()
public class ServiceMongoDB {
    @WebMethod(operationName = "getQuery")
    public String getQuery(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "dbName")
    String dbName, @WebParam(name = "collectionName")
    String collectionName, @WebParam(name = "query")
    String query) {
        String res="";
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( dbName );
        DBCollection coll =  db.getCollection(collectionName);

        DBObject q = (DBObject) JSON.parse(query);

        DBCursor cursor = coll.find(q);
        while (cursor.hasNext()) {
            res=res+cursor.next()+"\n";
        }
        cursor.close();
        mongoClient.close();
        return res;
    }

    /**
     * Web service creating database
     */
    @WebMethod(operationName = "createDB")
    public Boolean createDB(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port,@WebParam(name = "dbName")
    String dbName) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( dbName );
        mongoClient.close();
        if (db!=null){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Web service creating collection
     */
    @WebMethod(operationName = "createCollection")
    public Boolean createCollection(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "dbName")
    String dbName, @WebParam(name = "nameCollection")
    String nameCollection) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( dbName );
        DBCollection coll =  db.getCollection(nameCollection);
        mongoClient.close();
        if (coll!=null){
            return true;
        }else{
            return false;
        }
    }

    /**
     * Web service inserting a document
     */
    @WebMethod(operationName = "insertDocument")
    public void insertDocument(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "nameDB")
    String nameDB, @WebParam(name = "nameCollection")
    String nameCollection, @WebParam(name = "document")
    String document) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( nameDB );
        DBCollection coll =  db.getCollection(nameCollection);
        DBObject ins = (DBObject) JSON.parse(document);
        coll.insert(ins);
        mongoClient.close();
    }

    /**
     * Web service deleting collection
     */
    @WebMethod(operationName = "deleteCollection")
    public void deleteCollection(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "nameDB")
    String nameDB, @WebParam(name = "nameCollection")
    String nameCollection) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( nameDB );
        DBCollection coll =  db.getCollection(nameCollection);
        coll.drop();
        mongoClient.close();
    }

    /**
     * Web service deleting database
     */
    @WebMethod(operationName = "deleteDatabase")
    public void deleteDatabase(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "nameDB")
    String nameDB) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( nameDB );
        db.dropDatabase();
        mongoClient.close();
    }

    /**
     * Web service deleting all documents in a collection
     */
    @WebMethod(operationName = "deleteAllDocuments")
    public void deleteAllDocuments(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "nameDB")
    String nameDB, @WebParam(name = "nameCollection")
    String nameCollection) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( nameDB );
        DBCollection coll =  db.getCollection(nameCollection);
        coll.remove(new BasicDBObject());
        mongoClient.close();
    }

    /**
     * Web service deleting a document
     */
    @WebMethod(operationName = "deleteDocument")
    public void deleteDocument(@WebParam(name = "ip")
    String ip, @WebParam(name = "port")
    int port, @WebParam(name = "nameDB")
    String nameDB, @WebParam(name = "nameCollection")
    String nameCollection, @WebParam(name = "query")
    String query) {
        MongoClient mongoClient = new MongoClient( new ServerAddress(ip, port)                                    );
        DB db = mongoClient.getDB( nameDB );
        DBCollection coll =  db.getCollection(nameCollection);
        DBObject q = (DBObject) JSON.parse(query);
        coll.remove(q);
        mongoClient.close();
    }
}
