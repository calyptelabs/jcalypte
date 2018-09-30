/*
 * Calypte http://calypte.uoutec.com.br/
 * Copyright (C) 2018 UoUTec. (calypte@uoutec.com.br)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package calypte.jcalypte;

class CalypteConnectionProxy implements CalypteConnection{

	private CalypteConnectionPool pool;
	
	private CalypteConnection con;
	
	private boolean closed;
	
	public CalypteConnectionProxy(CalypteConnection con, 
			CalypteConnectionPool pool) throws CacheException {
		this.pool   = pool;
		this.con    = con;
		this.closed = false;
	}

	public void close() throws CacheException {
		if(!this.closed){
			this.pool.release(con);
			this.closed = true;
		}
	}

	public boolean isClosed() {
		return this.closed;
	}

	public boolean replace(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.replace(key, value, timeToLive,
				timeToIdle);
	}

	public boolean replace(String key, Object oldValue, Object newValue,
			long timeToLive, long timeToIdle) throws CacheException {
		return this.con.replace(key, oldValue, newValue,
				timeToLive, timeToIdle);
	}

	public Object putIfAbsent(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.putIfAbsent(key, value, timeToLive,
				timeToIdle);
	}

	public boolean set(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.set(key, value, timeToLive,
				timeToIdle);
	}

	public boolean put(String key, Object value, long timeToLive,
			long timeToIdle) throws CacheException {
		return this.con.put(key, value, timeToLive,
				timeToIdle);
	}

	public Object get(String key, boolean forUpdate) throws CacheException {
		return this.con.get(key, forUpdate);
	}

	public Object get(String key) throws CacheException {
		return this.con.get(key);
	}

	public boolean remove(String key, Object value) throws CacheException {
		return this.con.remove(key, value);
	}

	public boolean remove(String key) throws CacheException {
		return this.con.remove(key);
	}

	public void setAutoCommit(boolean value) throws CacheException {
		this.con.setAutoCommit(value);
	}

	public boolean isAutoCommit() throws CacheException {
		return con.isAutoCommit();
	}

	public void commit() throws CacheException {
		con.commit();
	}

	public void rollback() throws CacheException {
		con.rollback();
	}

	public void flush() throws CacheException {
		con.flush();
	}
	
	public String getHost() {
		return this.con.getHost();
	}

	public int getPort() {
		return this.con.getPort();
	}

    protected void finalize() throws Throwable {
		try{
			this.close();
		}
		finally{
			super.finalize();
		}
	}

}
