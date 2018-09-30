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

import java.net.Socket;
import java.util.zip.CRC32;

/**
 * Permite o armazenamento, atualização, remoção de um item no Calypte.
 * 
 * @author Ribeiro.
 */
class CalypteConnectionImp implements CalypteConnection{
	
    public static final byte[] CRLF_DTA                  = "\r\n".getBytes();

    public static final byte[] DEFAULT_FLAGS_DTA         = "0".getBytes();
    
    public static final byte[] SHOW_VAR                  = "show_var".getBytes();

    public static final byte[] SET_VAR                   = "set_var".getBytes();
    
    public static final byte[] SHOW_VARS                 = "show_vars".getBytes();
    
    public static final byte[] BOUNDARY_DTA              = "end".getBytes();
    
    public static final byte[] PUT_COMMAND_DTA           = "put".getBytes();

    public static final byte[] REPLACE_COMMAND_DTA       = "replace".getBytes();

    public static final byte[] SET_COMMAND_DTA           = "set".getBytes();
    
    public static final byte[] GET_COMMAND_DTA           = "get".getBytes();

    public static final byte[] REMOVE_COMMAND_DTA        = "remove".getBytes();

    public static final byte[] BEGIN_TX_COMMAND_DTA      = "begin".getBytes();

    public static final byte[] FLUSH_COMMAND_DTA         = "flush".getBytes();
    
    public static final byte[] COMMIT_TX_COMMAND_DTA     = "commit".getBytes();

    public static final byte[] ROLLBACK_TX_COMMAND_DTA   = "rollback".getBytes();
    
    public static final byte[] ERROR_DTA                 = "error".getBytes();
    
    public static final byte[] VALUE_RESULT_DTA          = "value".getBytes();
    
    public static final byte[] SUCCESS_DTA               = "ok".getBytes();

    public static final byte[] PUT_SUCCESS_DTA           = "stored".getBytes();
    
    public static final byte[] REPLACE_SUCCESS_DTA       = "replaced".getBytes();

    public static final byte[] NOT_STORED_DTA            = "not_stored".getBytes();
    
    public static final byte[] NOT_FOUND_DTA             = "not_found".getBytes();
    
    public static final byte[] SEPARATOR_COMMAND_DTA     = " ".getBytes();

    public static final String ENCODE                    = "UTF-8";
    
    protected String host;
    
    protected int port;
    
    protected Socket socket;
    
    protected StreamFactory streamFactory;
    
    protected CalypteSender sender;

    protected CalypteReceiver receiver;
    
    protected boolean closed;
    
    /**
     * Cria uma nova instância de {@link CalypteConnection}
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     * @throws CacheException Lançada se não for possível a conexão com o servidor.
     */
    public CalypteConnectionImp(String host, int port) throws CacheException{
        this(host, port, new DefaultStreamFactory());
    }
    
    /**
     * Cria uma nova instância de {@link CalypteConnection}
     * @param host Endereço do servidor.
     * @param port Porta que o servidor está escutando.
     * @throws CacheException Lançada se não for possível a conexão com o servidor.
     */
    public CalypteConnectionImp(String host, int port, StreamFactory streamFactory) throws CacheException{
        this.host 			= host;
        this.port 			= port;
        this.streamFactory 	= streamFactory;
        this.connect();
    }
    
    public void connect() throws CacheException{
    	try{
	        this.socket     = new Socket(this.getHost(), this.getPort());
	        
	        this.socket.setTcpNoDelay(true);
	        this.socket.setTcpNoDelay(true);
	        this.socket.setSendBufferSize(8*1024);
	        this.socket.setReceiveBufferSize(8*1024);
	        this.socket.setKeepAlive(false);
	        this.socket.setOOBInline(true);
	        
	        this.sender     = new CalypteSender(socket, streamFactory, 8*1024);
	        this.receiver   = new CalypteReceiver(socket, streamFactory, 8*1024);
	        this.closed     = false;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    }
    
    public void close() throws CacheException{
        
    	try{
	        if(this.socket != null){
	            this.socket.close();
	        }
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    	finally{
	        this.closed   = true;
	        this.sender   = null;
	        this.receiver = null;
    	}
    }
    
    public boolean isClosed(){
    	return this.closed;
    }
    
	/* métodos de armazenamento */

	public boolean replace(
			String key, Object value, long timeToLive, long timeToIdle) throws CacheException{
		
		try{
			this.sender.executeReplace(key, timeToLive, timeToIdle, value);
			return this.receiver.processReplaceResult();
		}
		catch(CacheException e){
			throw e;
		}
		catch(Throwable e){
    		throw new CacheException(e);
		}
		
	}
    
	public boolean replace(
			String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws CacheException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			boolean result;
			if(o != null && o.equals(oldValue)){
				result = this.put(key, newValue, timeToLive, timeToIdle);
			}
			else
				result = false;
			
			this.commitLocalTransaction(localTransaction);
			return result;
		}
		catch(Throwable e){
			throw this.rollbackLocalTransaction(localTransaction, e);
		}
		
	}
	
	public Object putIfAbsent(
			String key, Object value, long timeToLive, long timeToIdle) throws CacheException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			this.set(key, value, timeToLive, timeToIdle);
			this.commitLocalTransaction(localTransaction);
			return o;
		}
		catch(Throwable e){
			throw this.rollbackLocalTransaction(localTransaction, e);
		}
		
	}
	
    public boolean put(String key, Object value, long timeToLive, long timeToIdle) 
            throws CacheException {

    	try{
	    	this.sender.executePut(key, timeToLive, timeToIdle, value);
	        return this.receiver.processPutResult();
    	}
		catch(CacheException e){
			throw e;
		}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
        
    }

    public boolean set(String key, Object value, long timeToLive, long timeToIdle) 
            throws CacheException {

    	try{
	    	this.sender.executeSet(key, timeToLive, timeToIdle, value);
	        return this.receiver.processSetResult();
    	}
		catch(CacheException e){
			throw e;
		}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
        
    }
    
	/* métodos de coleta*/
    
    public Object get(String key, boolean forUpdate) throws CacheException{
    	
    	try{
	    	this.sender.executeGet(key, forUpdate);
	        return this.receiver.processGetResult();
    	}
		catch(CacheException e){
			throw e;
		}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    	
    }
    
    public Object get(String key) throws CacheException{
    	return this.get(key, false);
    }

    /* métodos de remoção */

	public boolean remove(
			String key, Object value) throws CacheException{
		
		Boolean localTransaction = null;
		
		try{
			localTransaction = this.startLocalTransaction();
			Object o = this.get(key, true);
			boolean result;
			if(o != null && o.equals(value)){
				result = this.remove(key);
			}
			else
				result = false;
			
			this.commitLocalTransaction(localTransaction);
			return result;
		}
		catch(Throwable e){
			throw this.rollbackLocalTransaction(localTransaction, e);
		}
		
	}
    
    public boolean remove(String key) throws CacheException{

    	try{
	    	this.sender.executeRemove(key);
	        return this.receiver.processRemoveResult();
    	}
		catch(CacheException e){
			throw e;
		}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    	
    }
    
    public void setAutoCommit(boolean value) throws CacheException{

    	try{
    		this.sender.executeSetVar("auto_commit", value);
    		this.receiver.processSetVarResult();
    	}
    	catch(CacheException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}

    }
    
    public boolean isAutoCommit() throws CacheException{
    	try{
    		this.sender.executeShowVar("auto_commit");
    		String var = this.receiver.processShowVarResult("auto_commit");
    		return var.equals("true");
    	}
    	catch(CacheException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    }
    
    public void commit() throws CacheException{
    	try{
    		this.sender.executeCommitTransaction();
    		this.receiver.processCommitTransactionResult();
    	}
    	catch(CacheException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    }
    
    public void rollback() throws CacheException{
    	try{
    		this.sender.executeRollbackTransaction();
    		this.receiver.processRollbackTransactionResult();
    	}
    	catch(CacheException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
    }

	public void flush() throws CacheException {
    	try{
    		sender.executeFlush();
    		receiver.processFlushResult();
    	}
    	catch(CacheException e){
    		throw e;
    	}
    	catch(Throwable e){
    		throw new CacheException(e);
    	}
	}
    
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public StreamFactory getStreamFactory() {
        return streamFactory;
    }
    
    private boolean startLocalTransaction() throws CacheException{
    	if(this.isAutoCommit()){
    		this.setAutoCommit(false);
    		return true;
    	}
    	
    	return false;
    }

    private void commitLocalTransaction(Boolean local) throws CacheException{
    	if(local != null && local){
    		this.commit();
    	}
    }

    private CacheException rollbackLocalTransaction(Boolean local, Throwable e){
    	
    	try{
        	if(local != null && local){
        		this.rollback();
        	}
		}
		catch(CacheException ex){
			return ex;
		}
		catch(Throwable ex){
			return new CacheException("rollback fail: " + ex.toString(), e);
		}
		
		if(e instanceof CacheException){
			 return (CacheException)e;
		}
		else{
    		return new CacheException(e);
		}
		
    }
    
    @SuppressWarnings("unused")
    @Deprecated
	private byte[] getCRC32(byte[] data, int off, int len){
        CRC32 crc32 = new CRC32();
        crc32.update(data, off, len);
        long crcValue = crc32.getValue();
        byte[] crc = new byte[8];

        crc[0] = (byte)(crcValue & 0xffL); 
        crc[1] = (byte)(crcValue >> 8  & 0xffL); 
        crc[2] = (byte)(crcValue >> 16 & 0xffL); 
        crc[3] = (byte)(crcValue >> 24 & 0xffL);
        crc[4] = (byte)(crcValue >> 32 & 0xffL); 
        crc[5] = (byte)(crcValue >> 40 & 0xffL); 
        crc[6] = (byte)(crcValue >> 48 & 0xffL); 
        crc[7] = (byte)(crcValue >> 56 & 0xffL);
        return crc;
    }
    
    protected void finalize() throws Throwable{
    	try{
    		this.close();
    	}
    	finally{
    		super.finalize();
    	}
    }

}
