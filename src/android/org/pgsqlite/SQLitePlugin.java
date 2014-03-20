/*
 * Copyright (c) 2012-2013, Chris Brody
 * Copyright (c) 2005-2010, Nitobi Software Inc.
 * Copyright (c) 2010, IBM Corporation
 */

package org.pgsqlite;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;

import java.lang.Number;

import java.util.HashMap;

import org.apache.cordova.CordovaPlugin;
import org.apache.cordova.CallbackContext;

import org.apache.cordova.file.FileUtils;
import java.net.MalformedURLException;

import android.database.Cursor;
import android.database.DatabaseErrorHandler;

import android.database.sqlite.*;

import android.util.Base64;
import android.util.Log;

public class SQLitePlugin extends CordovaPlugin
{

	public static final String TAG = "SQLitePlugin";

	/**
	 * Multiple database map (static).
	 */
	static HashMap<String, SQLiteDatabase> dbmap = new HashMap<String, SQLiteDatabase>();

	/**
	 * Get a SQLiteDatabase reference from the db map (public static accessor).
	 *
	 * @param dbname
	 *            The name of the database.
	 *
	 */
	public static SQLiteDatabase getSQLiteDatabase(String dbname)
	{
		return dbmap.get(dbname);
	}

	/**
	 * NOTE: Using default constructor, explicit constructor no longer required.
	 */

	/**
	 * Executes the request and returns PluginResult.
	 *
	 * @param action
	 *            The action to execute.
	 *
	 * @param args
	 *            JSONArry of arguments for the plugin.
	 *
	 * @param cbc
	 *            Callback context from Cordova API
	 *
	 */
	@Override
	public boolean execute(String action, JSONArray args, CallbackContext cbc)
	{
		try {
			boolean status = true;

			if (action.equals("open")) {
				JSONObject o = args.getJSONObject(0);
				String dbname = o.getString("name");

				if (o.optInt("bgType", 0) == 1)
					this.openDatabaseAsync(dbname, cbc);
				else
					this.openDatabase(dbname, cbc);
			}
			else if (action.equals("close")) {
				JSONObject o = args.getJSONObject(0);
				String dbname = o.getString("path");

				this.closeDatabase(dbname);
			}
			else if (action.equals("delete")) {
				/* Stop & give up if API < 16: */
				if (android.os.Build.VERSION.SDK_INT < 16) return false;

				JSONObject o = args.getJSONObject(0);
				String dbname = o.getString("path");

				status = this.deleteDatabase(dbname);
			}
			else if (action.equals("executePragmaStatement"))
			{
				String dbName = args.getString(0);
				String query = args.getString(1);

				JSONArray jparams = (args.length() < 3) ? null : args.getJSONArray(2);

				String[] params = null;

				if (jparams != null) {
					params = new String[jparams.length()];

					for (int j = 0; j < jparams.length(); j++) {
						if (jparams.isNull(j))
							params[j] = "";
						else
							params[j] = jparams.getString(j);
					}
				}

				Cursor myCursor = this.getDatabase(dbName).rawQuery(query, params);

				String result = this.getRowsResultFromQuery(myCursor).getJSONArray("rows").toString();

				this.sendJavascriptCB("window.SQLitePluginCallback.p1('" + id + "', " + result + ");");
			}
			else if (action.equals("backgroundExecuteSqlBatch")) {
					this.executeSqlBatchInBackground(args, cbc);
			}
			else if (action.equals("executeSqlBatch")) {
					this.executeSqlBatch(args, cbc, false);
			}

			return status;
		} catch (JSONException e) {
			// TODO: signal JSON problem to JS

			return false;
		}
	}

	/**
	 *
	 * Clean up and close all open databases.
	 *
	 */
	@Override
	public void onDestroy() {
		while (!dbmap.isEmpty()) {
			String dbname = dbmap.keySet().iterator().next();
			this.closeDatabase(dbname);
			dbmap.remove(dbname);
		}
	}

	// --------------------------------------------------------------------------
	// LOCAL METHODS
	// --------------------------------------------------------------------------

	/**
	 * Open a database, asyncronously
	 *
	 * @param args
	 *            plugin arguments
	 *
	 * @param cbc
	 *            Callback context from Cordova API
	 *
	 */
	private void openDatabaseAsync(final String dbname, final CallbackContext cbc)
	{
		cordova.getThreadPool().execute(new Runnable() {
			public void run() {
				synchronized(dbmap) {
					openDatabase(dbname, cbc);
				}
			}
		});
	}

	/**
	 * Open a database.
	 *
	 * @param args
	 *            plugin arguments
	 *
	 * @param cbc
	 *            Callback context from Cordova API
	 *
	 */
	private void openDatabase(String dbname, CallbackContext cbc)
	{
		if (this.getDatabase(dbname) != null)
			this.closeDatabase(dbname);

		File dbfile;
		FileUtils filePlugin = (FileUtils) webView.pluginManager.getPlugin("File");

		try { // Try to resolve dbname to abs path
			dbfile = new File(filePlugin.filesystemPathForURL(dbname));
		} catch (MalformedURLException e) {
			// The filesystem url wasn't recognized
			// follow the old behaviour
			dbfile = this.cordova.getActivity().getDatabasePath(dbname + ".db");
		}

		String dbpath = dbfile.getAbsolutePath();

		if (!dbfile.exists()) {
			dbfile.getParentFile().mkdirs();
		}

		Log.d(TAG, "openDatabase: " + dbpath);

		try {
			SQLiteDatabase mydb = SQLiteDatabase.openDatabase(
				dbpath,
				null,
				SQLiteDatabase.CREATE_IF_NECESSARY,
				new DatabaseErrorHandler () {
					public void onCorruption(SQLiteDatabase dbObj) {
						// prevent from automatic file deletion
						throw new SQLiteException("file is corrupted");
					}
				}
			);

			dbmap.put(dbname, mydb);

			cbc.success();
		} catch (SQLiteException ex) {
			String msg = ex.getMessage();
			Log.d(TAG, "openDatabase: Error " + dbpath + " " + msg);
			try {
				JSONObject er = new JSONObject();
				er.put("message", msg);

				cbc.error(er);
			} catch (JSONException x) {
				Log.e(TAG, "openDatabase failure creating error message ");
			}
		}
	}

	/**
	 * Close a database.
	 *
	 * @param dbName
	 *            The name of the database-NOT including its extension.
	 *
	 */
	private void closeDatabase(String dbName)
	{
		SQLiteDatabase mydb = this.getDatabase(dbName);

		if (mydb != null)
		{
			mydb.close();
			dbmap.remove(dbName);
		}
	}

	/**
	 * Delete a database.
	 *
	 * @param dbname
	 *            The name of the database-NOT including its extension.
	 *
	 * @return true if successful or false if an exception was encountered
	 *
	 */
	private boolean deleteDatabase(String dbname)
	{
		boolean status = false; // assume the worst case:

		if (this.getDatabase(dbname) != null) this.closeDatabase(dbname);

		File dbfile = this.cordova.getActivity().getDatabasePath(dbname + ".db");

		Log.v("info", "delete sqlite db: " + dbfile.getAbsolutePath());

		// Use try & catch just in case android.os.Build.VERSION.SDK_INT >= 16 was lying:
		try {
			status = SQLiteDatabase.deleteDatabase(dbfile);
		} catch (Exception ex) {
			// log & give up:
			Log.v("executeSqlBatch", "deleteDatabase(): Error=" +  ex.getMessage());
			ex.printStackTrace();
		}

		return status;
	}

	/**
	 * Get a database from the db map.
	 *
	 * @param dbname
	 *            The name of the database.
	 *
	 */
	private SQLiteDatabase getDatabase(String dbname)
	{
		return dbmap.get(dbname);
	}

	/**
	 * Executes a batch request IN BACKGROUND THREAD and sends the results back.
	 *
	 * @param args
	 *            query arguments (from plugin exec)
	 *
	 * @param cbc
	 *            Callback context from Cordova API
	 *
	 */
	private void executeSqlBatchInBackground(final JSONArray args, final CallbackContext cbc)
	{
		final SQLitePlugin myself = this;

		cordova.getThreadPool().execute(new Runnable() {
			public void run() {
				synchronized(myself) {
					executeSqlBatch(args, cbc, true);
				}
			}
		});
	}

	/**
	 * Executes a batch request and sends the results back.
	 *
	 * @param args
	 *            query arguments (from plugin exec)
	 *
	 * @param cbc
	 *            Callback context from Cordova API
	 *
	 */
	private void executeSqlBatch(JSONArray args, CallbackContext cbc, boolean bgWorkaround)
	{
		String errorMessage = "";
		String query = "";
		String query_id = "";
		SQLiteDatabase mydb = null;
		JSONArray batchResults = null;

		try {
			JSONObject allargs = args.getJSONObject(0);
			JSONObject dbargs = allargs.getJSONObject("dbargs");
			String dbName = dbargs.getString("dbname");
			JSONArray txQueue = allargs.optJSONArray("executes");
			if (txQueue != null && txQueue.isNull(0))
				txQueue = null;

			mydb = this.getDatabase(dbName);

			batchResults = new JSONArray();

			int len = txQueue == null ? 0 : txQueue.length();
			for (int i = 0; i < len; i++) {
				JSONObject queryJSON = txQueue.getJSONObject(i);

				query_id = queryJSON.getString("qid");
				query = queryJSON.getString("sql");
				JSONArray jsonparams = queryJSON.optJSONArray("params");

				JSONObject queryResult = null;

				String[] qSplit = query.split("\\W");
				String command = qSplit.length > 0 ? qSplit[0].toUpperCase() : "";

				Log.d(TAG, "executeSqlBatch "+dbName+": ("+ command+ ") " +query);

				if (mydb == null) {
					Log.d(TAG, "executeSqlBatch Db is not opened: " + dbName);
				}

				queryResult = new JSONObject();

				// UPDATE or DELETE:
				// NOTE: this code should be safe to RUN with old Android SDK.
				// To BUILD with old Android SDK remove lines from HERE: {{
				if (android.os.Build.VERSION.SDK_INT >= 11 &&
					("UPDATE".equals(command) || "DELETE".equals(command)))
				{
					SQLiteStatement myStatement = mydb.compileStatement(query);

					pushParams(myStatement, jsonparams);

					// Use try & catch just in case android.os.Build.VERSION.SDK_INT >= 11 is lying:
					try {
						int rowsAffected = myStatement.executeUpdateDelete();

						queryResult.put("rowsAffected", rowsAffected);
					} catch (SQLiteException ex) {
						// Indicate problem & stop this query:
						throw ex;

						//~ ex.printStackTrace();
						//~ errorMessage = ex.getMessage();
						//~ Log.d(TAG, "executeSqlBatch executeUpdateDelete(): Error=" +  errorMessage);

					//~ } catch (Exception ex) {
						//~ // Assuming SDK_INT was lying & method not found:
						//~ // do nothing here & try again with raw query.
					}
				}
				else // to HERE. }}

				if ("INSERT".equals(command) && jsonparams != null) {

					SQLiteStatement myStatement = mydb.compileStatement(query);

					pushParams(myStatement, jsonparams);

					long insertId = myStatement.executeInsert();
					queryResult.put("insertId", insertId);
					queryResult.put("rowsAffected", insertId != -1 ? 1 : 0);
				}
				else if ("BEGIN".equals(command)) {
					mydb.beginTransaction();

					queryResult.put("rowsAffected", 0);
				}
				else if ("COMMIT".equals(command)) {
					if (mydb.inTransaction()) {
						mydb.setTransactionSuccessful();
						mydb.endTransaction();
					}

					queryResult.put("rowsAffected", 0);
				}
				else if ("ROLLBACK".equals(command)) {
					mydb.endTransaction();

					queryResult.put("rowsAffected", 0);
				}
				else {
					// raw query for other statements:
					String[] params = null;

					if (jsonparams != null) {
						params = new String[jsonparams.length()];

						for (int j = 0; j < jsonparams.length(); j++) {
							if (jsonparams.isNull(j))
								params[j] = "";
							else
								params[j] = jsonparams.getString(j);
						}
					}

					Cursor myCursor = null;
					try {
						myCursor = mydb.rawQuery(query, params);

						if (query_id.length() > 0) {
							queryResult = getRowsResultFromQuery(myCursor);
						}
					} finally {
						if (myCursor != null)
							myCursor.close();
					}
				}

				JSONObject r = new JSONObject();
				r.put("qid", query_id);

				r.put("type", "success");
				r.put("result", queryResult);

				batchResults.put(r);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			errorMessage = ex.getMessage();
			Log.e(TAG, "executeSqlBatch error executing " +  query + ": " + errorMessage);

			JSONObject r = new JSONObject();

			try {
				r.put("qid", query_id);
				r.put("type", "error");

				JSONObject er = new JSONObject();
				er.put("message", errorMessage);
				r.put("result", er);
			} catch (JSONException x) {
				Log.e(TAG, "executeSqlBatch error creating error message ");
			}

			batchResults.put(r);
		} finally {
			// SQLitePlugin.js sends COMMIT in a separate batch,
			// so when a call is asyncronous a transaction finishes abruptly
			if (bgWorkaround && mydb != null && mydb.inTransaction()) {
				if ("".equals(errorMessage)) {
					mydb.setTransactionSuccessful();
				}
				Log.d(TAG, "executeSqlBatch bgWorkaround endTransaction " + errorMessage);
				mydb.endTransaction();
			}
		}

		cbc.success(batchResults);
	}

	/**
	 * Push parameters into SQL statement.
	 *
	 * @param myStatement
	 *            Compled SQL statement
	 *
	 * @param jsonparams
	 *            SQL statement parameters
	 *
	 */
	private void pushParams(SQLiteStatement myStatement, JSONArray jsonparams)
	throws JSONException
	{
		if (jsonparams == null)
			return;

		for (int j = 0; j < jsonparams.length(); j++) {
			if (jsonparams.get(j) instanceof Float || jsonparams.get(j) instanceof Double ) {
				myStatement.bindDouble(j + 1, jsonparams.getDouble(j));
			} else if (jsonparams.get(j) instanceof Number) {
				myStatement.bindLong(j + 1, jsonparams.getLong(j));
			} else if (jsonparams.isNull(j)) {
				myStatement.bindNull(j + 1);
			} else {
				String strParm = jsonparams.getString(j);
				// try to parse dataURI
				if (strParm.startsWith("data:")) {
					final String b64_patt = ";base64,";
					int b64_idx = strParm.indexOf(b64_patt);
					if (b64_idx != -1) {
						try {
							String blob64 = strParm.substring(b64_idx + b64_patt.length());
							byte[] blob = Base64.decode(blob64, Base64.DEFAULT);
							myStatement.bindBlob(j + 1, blob);
							strParm = null;
						} catch (IllegalArgumentException ex) {
						}
					}
				}
				// it's not a dataURI
				if (null != strParm)
					myStatement.bindString(j + 1, strParm);
			}
		}
	}

	/**
	 * Get rows results from query cursor.
	 *
	 * @param cur
	 *            Cursor into query results
	 *
	 * @return results in string form
	 *
	 */
	private JSONObject getRowsResultFromQuery(Cursor cur)
	{
		JSONObject rowsResult = new JSONObject();

		// If query result has rows
		if (cur.moveToFirst()) {
			JSONArray rowsArrayResult = new JSONArray();
			String key = "";
			int colCount = cur.getColumnCount();

			// Build up JSON result object for each row
			do {
				JSONObject row = new JSONObject();
				try {
					for (int i = 0; i < colCount; ++i) {
						key = cur.getColumnName(i);

						// NOTE: this code should be safe to RUN with old Android SDK.
						// To BUILD with old Android SDK remove lines from HERE: {{
						if(android.os.Build.VERSION.SDK_INT >= 11)
						{
							int curType = 3; /* Cursor.FIELD_TYPE_STRING */

							// Use try & catch just in case android.os.Build.VERSION.SDK_INT >= 11 is lying:
							try {
								curType = cur.getType(i);

								switch(curType)
								{
								case Cursor.FIELD_TYPE_NULL:
									row.put(key, JSONObject.NULL);
									break;
								case Cursor.FIELD_TYPE_INTEGER:
									row.put(key, cur.getInt(i));
									break;
								case Cursor.FIELD_TYPE_FLOAT:
									row.put(key, cur.getFloat(i));
									break;
								case Cursor.FIELD_TYPE_BLOB:
									row.put(key, new String(Base64.encode(cur.getBlob(i), Base64.DEFAULT)));
									break;
								case Cursor.FIELD_TYPE_STRING:
								default: /* (not expected) */
									row.put(key, cur.getString(i));
									break;
								}

							} catch (Exception ex) {
								// simply treat like a string
								row.put(key, cur.getString(i));
							}
						}
						else // to HERE. }}
						{
							row.put(key, cur.getString(i));
						}
					}

					rowsArrayResult.put(row);

				} catch (JSONException e) {
					e.printStackTrace();
				}

			} while (cur.moveToNext());

			try {
				rowsResult.put("rows", rowsArrayResult);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}

		return rowsResult;
	}

	/**
	 * Send Javascript callback.
	 *
	 * @param cb
	 *            Javascript callback command to send
	 *
	 */
	private void sendJavascriptCB(String cb)
	{
		this.webView.sendJavascript(cb);
	}

	/**
	 * Send Javascript callback on GUI thread.
	 *
	 * @param cb
	 *            Javascript callback command to send
	 *
	 */
	private void sendJavascriptToGuiThread(final String cb)
	{
		final SQLitePlugin myself = this;

		this.cordova.getActivity().runOnUiThread(new Runnable() {
			public void run() {
				myself.webView.sendJavascript(cb);
			}
		});
	}
}
