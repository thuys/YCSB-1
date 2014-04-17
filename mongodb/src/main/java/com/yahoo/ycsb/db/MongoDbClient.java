/**
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_db.java
 *
 */

package com.yahoo.ycsb.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MongoDB client for YCSB framework.
 * 
 * Properties to set:
 * 
 * mongodb.url=mongodb://localhost:27017 mongodb.database=ycsb
 * mongodb.writeConcern=normal
 * 
 * @author ypai
 */
public class MongoDbClient extends DB {

	/** Used to include a field in a response. */
	protected static final Integer INCLUDE = Integer.valueOf(1);

	/** A singleton Mongo instance. */
	private Mongo mongo;

	/** The default write concern for the test. */
	private WriteConcern writeConcern;

	/** The database to access. */
	private String database;

	/**
	 * Count the number of times initialized to teardown on the last
	 * {@link #cleanup()}.
	 */
	private static final AtomicInteger initCount = new AtomicInteger(0);

	private int number;

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	@Override
	public void init() throws DBException {
		synchronized (INCLUDE) {
			number = initCount.get();
			initCount.incrementAndGet();

			// initialize MongoDb driver
			Properties props = getProperties();
			String url = props.getProperty("mongodb.url",
					"mongodb://localhost:27017");
			database = props.getProperty("mongodb.database", "ycsb");
			String writeConcernType = props.getProperty("mongodb.writeConcern",
					"safe").toLowerCase();
			String readPreferenceS = props.getProperty(
					"mongodb.readPreference", "primary").toLowerCase();
			String connectionPref = props.getProperty(
					"mongodb.connections", "one").toLowerCase();
			final String maxConnections = props.getProperty(
					"mongodb.maxconnections", "10");

			if ("none".equals(writeConcernType)) {
				writeConcern = WriteConcern.NONE;
			} else if ("safe".equals(writeConcernType)) {
				writeConcern = WriteConcern.SAFE;
			} else if ("normal".equals(writeConcernType)) {
				writeConcern = WriteConcern.NORMAL;
			} else if ("fsync_safe".equals(writeConcernType)) {
				writeConcern = WriteConcern.FSYNC_SAFE;
			} else if ("replicas_safe".equals(writeConcernType)) {
				writeConcern = WriteConcern.REPLICAS_SAFE;
			} else if ("majority".equals(writeConcernType)) {
				writeConcern = WriteConcern.MAJORITY;
			} else {
				System.err
						.println("ERROR: Invalid writeConcern: '"
								+ writeConcernType
								+ "'. "
								+ "Must be [ none | safe | normal | fsync_safe | replicas_safe ]");
				System.exit(1);
			}
			ReadPreference readPreference = null;

			if ("nearest".equals(readPreferenceS)) {
				readPreference = ReadPreference.nearest();
			} else if ("primary".equals(readPreferenceS)) {
				readPreference = ReadPreference.primary();
			} else if ("primarypreferred".equals(readPreferenceS)) {
				readPreference = ReadPreference.primaryPreferred();
			} else if ("secondary".equals(readPreferenceS)) {
				readPreference = ReadPreference.secondary();
			} else if ("secondarypreferred".equals(readPreferenceS)) {
				readPreference = ReadPreference.secondaryPreferred();
			} else {
				System.err
						.println("ERROR: Invalid readPreference: '"
								+ readPreferenceS
								+ "'. "
								+ "Must be [ nearest | primary | primaryPreferred | secondary | secondaryPreferred ]");
				System.exit(1);
			}
			MongoOptions options = new MongoOptions();
			options.connectionsPerHost = Integer.parseInt(maxConnections);
			options.readPreference = readPreference;

			String[] urls = url.split(",");
			ArrayList<ServerAddress> servers = new ArrayList<ServerAddress>();
			
			try {
				if(connectionPref.equals("one")){
					String singleUrl = urls[number % urls.length];
					
					servers.add(convertToServerAddress(singleUrl));
					
				}else if(connectionPref.equals("all")){
					for(String singleUrl: urls){
						servers.add(convertToServerAddress(singleUrl));
					}
				}else{
					System.err
					.println("ERROR: Invalid mongodb.connections: '"
							+ connectionPref
							+ "'. "
							+ "Must be [ all | one ]");
					System.exit(1);
				}
			} catch (UnknownHostException e) {
				System.err
				.println("Could not initialize MongoDB connection pool for Loader: "
						+ e.toString());
			}

			try {
								// options.readPreference =new Read
				mongo = new Mongo(servers, options);
			} catch (Exception e1) {
				System.err
						.println("Could not initialize MongoDB connection pool for Loader: "
								+ e1.toString());
				// e1.printStackTrace();
			}

		}
	}
	private ServerAddress convertToServerAddress(String text) throws UnknownHostException{
		String singleURL = text.trim();
		// strip out prefix since Java driver doesn't currently
		// support
		// standard connection format URL yet
		// http://www.mongodb.org/display/DOCS/Connections
		if (singleURL.startsWith("mongodb://")) {
			singleURL = singleURL.substring(10);
		}

		// need to append db to url.
		singleURL += "/" + database;
		System.out.println("new database url = " + singleURL);
		
		return new DBAddress(singleURL);
	}
	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	@Override
	public void cleanup() throws DBException {
		synchronized (INCLUDE) {
			try {
				mongo.close();
			} catch (Exception e1) {
				System.err.println("Could not close MongoDB connection pool: "
						+ e1.toString());
				e1.printStackTrace();
				return;
			}
		}
	}

	public Mongo getDb() {
		return mongo;
	}

	/**
	 * Delete a record from the database.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int delete(String table, String key) {
		com.mongodb.DB db = null;
		try {
			db = getDb().getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection(table);
			DBObject q = new BasicDBObject().append("_id", key);
			WriteResult res = collection.remove(q, writeConcern);
			return res.getN() == 1 ? 0 : 1;
		} catch (Exception e) {
			System.err.println(e.toString());
			return 1;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to insert.
	 * @param values
	 *            A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> values) {
		com.mongodb.DB db = null;
		try {
			db = getDb().getDB(database);

			db.requestStart();

			DBCollection collection = db.getCollection(table);
			DBObject r = new BasicDBObject().append("_id", key);
			for (String k : values.keySet()) {
				r.put(k, values.get(k).toArray());
			}
			WriteResult res = collection.insert(r, writeConcern);
			return res.getError() == null ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			return 1;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}
	}

	/**
	 * Read a record from the database. Each field/value pair from the result
	 * will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to read.
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error or "not found".
	 */
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		com.mongodb.DB db = null;
		try {
			db = getDb().getDB(database);

			db.requestStart();

			DBCollection collection = db.getCollection(table);
			DBObject q = new BasicDBObject().append("_id", key);
			DBObject fieldsToReturn = new BasicDBObject();

			DBObject queryResult = null;
			if (fields != null) {
				Iterator<String> iter = fields.iterator();
				while (iter.hasNext()) {
					fieldsToReturn.put(iter.next(), INCLUDE);
				}
				queryResult = collection.findOne(q, fieldsToReturn);
			} else {
				queryResult = collection.findOne(q);
			}

			if (queryResult != null) {
				for (String field : queryResult.keySet()) {
					Object o = queryResult.get(field);
					if (o instanceof String) {
						result.put(field, new StringByteIterator((String) o));
					} else {
						String value = new String(
								(byte[]) (queryResult.get(field)));
						// System.out.println(field + " " + value);
						result.put(field, new StringByteIterator(value));
					}

				}
			}
			return queryResult != null ? 0 : 1;
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.toString());
			return 1;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}
	}

	/**
	 * Update a record in the database. Any field/value pairs in the specified
	 * values HashMap will be written into the record with the specified record
	 * key, overwriting any existing values with the same field name.
	 * 
	 * @param table
	 *            The name of the table
	 * @param key
	 *            The record key of the record to write.
	 * @param values
	 *            A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> values) {
		com.mongodb.DB db = null;
		try {
			db = getDb().getDB(database);

			db.requestStart();

			DBCollection collection = db.getCollection(table);
			DBObject q = new BasicDBObject().append("_id", key);
			DBObject u = new BasicDBObject();
			DBObject fieldsToSet = new BasicDBObject();
			Iterator<String> keys = values.keySet().iterator();
			while (keys.hasNext()) {
				String tmpKey = keys.next();
				fieldsToSet.put(tmpKey, values.get(tmpKey).toArray());

			}
			u.put("$set", fieldsToSet);
			WriteResult res = collection.update(q, u, false, false,
					writeConcern);
			return res.getN() == 1 ? 0 : 1;
		} catch (Exception e) {
			System.err.println(e.toString());
			return 1;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}
	}

	/**
	 * Perform a range scan for a set of records in the database. Each
	 * field/value pair from the result will be stored in a HashMap.
	 * 
	 * @param table
	 *            The name of the table
	 * @param startkey
	 *            The record key of the first record to read.
	 * @param recordcount
	 *            The number of records to read
	 * @param fields
	 *            The list of fields to read, or null for all of them
	 * @param result
	 *            A Vector of HashMaps, where each HashMap is a set field/value
	 *            pairs for one record
	 * @return Zero on success, a non-zero error code on error. See this class's
	 *         description for a discussion of error codes.
	 */
	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		com.mongodb.DB db = null;
		try {
			db = getDb().getDB(database);
			db.requestStart();
			DBCollection collection = db.getCollection(table);
			// { "_id":{"$gte":startKey, "$lte":{"appId":key+"\uFFFF"}} }
			DBObject scanRange = new BasicDBObject().append("$gte", startkey);
			DBObject q = new BasicDBObject().append("_id", scanRange);
			DBCursor cursor = collection.find(q).limit(recordcount);
			while (cursor.hasNext()) {
				// toMap() returns a Map, but result.add() expects a
				// Map<String,String>. Hence, the suppress warnings.
				HashMap<String, ByteIterator> resultMap = new HashMap<String, ByteIterator>();

				DBObject obj = cursor.next();
				fillMap(resultMap, obj);

				result.add(resultMap);
			}

			return 0;
		} catch (Exception e) {
			System.err.println(e.toString());
			return 1;
		} finally {
			if (db != null) {
				db.requestDone();
			}
		}

	}

	/**
	 * TODO - Finish
	 * 
	 * @param resultMap
	 * @param obj
	 */
	@SuppressWarnings("unchecked")
	protected void fillMap(HashMap<String, ByteIterator> resultMap, DBObject obj) {
		Map<String, Object> objMap = obj.toMap();
		for (Map.Entry<String, Object> entry : objMap.entrySet()) {
			if (entry.getValue() instanceof byte[]) {
				resultMap.put(entry.getKey(), new ByteArrayByteIterator(
						(byte[]) entry.getValue()));
			}
		}
	}
}