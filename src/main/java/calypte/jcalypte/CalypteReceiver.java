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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class CalypteReceiver {

    private BufferedInputStream in;
	
	public CalypteReceiver(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.in =
    			new BufferedInputStream(bufferLength, streamFactory.createInpuStream(socket));
	}

	public boolean processPutResult() throws IOException, CacheException{
		
		/*
		 * stored | replaced | <error>
		 */

		byte[] result = this.getLine();
		
		switch (result[0]) {
			case 's':
				return false;
			case 'r':
				return true;
			default:
				Error err = this.parseError(result);
				throw new CacheException(err.code, err.message);
		}
		
	}

	public boolean processReplaceResult() throws IOException, CacheException{
		
		/*
		 * replaced | not_stored | <error>
		 */
		
		byte[] result = this.getLine();
		
		switch (result[0]) {
			case 'r':
				return true;
			case 'n':
				return false;
			default:
				Error err = this.parseError(result);
				throw new CacheException(err.code, err.message);
		}
		
	}
	
	public Object processGetResult() throws IOException, CacheException, ClassNotFoundException{
		byte[] header = this.getLine();
		
		if(ArraysUtil.startsWith(header, CalypteConnectionImp.VALUE_RESULT_DTA)){
			CacheEntry e = this.getObject(header);
			
			byte[] boundary = this.getLine();
			
			if(!Arrays.equals(CalypteConnectionImp.BOUNDARY_DTA, boundary)){
				throw new IOException("expected end");
			}
			
			return e == null? null : this.toObject(e.getData());
		}
		else{
			Error err = this.parseError(header);
			throw new CacheException(err.code, err.message);
		}
	}

	public Map<String,Object> processGetsResult() throws IOException, CacheException, ClassNotFoundException{
		
		Map<String,Object> result = new HashMap<String, Object>();
		
		byte[] header = this.getLine();
		
		while(ArraysUtil.startsWith(header, CalypteConnectionImp.VALUE_RESULT_DTA)){
			CacheEntry e = this.getObject(header);
			if(e != null){
				result.put(e.getKey(), this.toObject(e.getData()));
			}
		}
		
		if(!Arrays.equals(CalypteConnectionImp.BOUNDARY_DTA, header)){
			Error err = this.parseError(header);
			throw new CacheException(err.code, err.message);
		}
		
		return result;
	}
	
	private CacheEntry getObject(byte[] header) throws IOException, CacheException{

		/*
		 * value <size> <flags>\r\n
		 * <data>\r\n
		 * end\r\n
		 */
		
		byte[][] dataParams = ArraysUtil.split(
				header, 
				CalypteConnectionImp.VALUE_RESULT_DTA.length + 1, 
				(byte)32);
		
		String key       = ArraysUtil.toString(dataParams[0]);
		int size         = ArraysUtil.toInt(dataParams[1]);
		int flags        = ArraysUtil.toInt(dataParams[2]);
		
		if(size > 0){
			byte[] dta = new byte[size];
			this.in.read(dta, 0, dta.length);
			
			byte[] endData = new byte[2];
			this.in.read(endData, 0, endData.length);
			
			if(!Arrays.equals(CalypteConnectionImp.CRLF_DTA, endData)){
				throw new IOException("corrupted data: " + key);
			}
			
			return new CacheEntry(key, size, flags, dta);
		}
		else{
			return null;
		}
		
	}

	public boolean processRemoveResult() throws IOException, CacheException{
		
		/*
		 * ok | not_found | <error>
		 */
		byte[] result = this.getLine();

		switch (result[0]) {
			case 'o':
				return true;
			case 'n':
				return false;
			default:
				Error err = this.parseError(result);
				throw new CacheException(err.code, err.message);
		}
		
	}

	public boolean processSetResult() throws IOException, CacheException{
		
		/*
		 * stored | not_stored | <error>
		 */
		byte[] result = this.getLine();

		switch (result[0]) {
			case 's':
				return true;
			case 'n':
				return false;
			default:
				Error err = this.parseError(result);
				throw new CacheException(err.code, err.message);
		}
		
	}
	
	public void processBeginTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}

	public void processCommitTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}

	public void processRollbackTransactionResult() throws IOException, CacheException{
		this.processDefaultTransactionCommandResult();
	}
	
	public void processDefaultTransactionCommandResult() throws IOException, CacheException{
		
		/*
		 * ok | <error>
		 */
		byte[] result = this.getLine();

		if(result[0] != 'o'){
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}

	public String processShowVarResult(String var) throws IOException, CacheException{
		
		/*
		 * ok | <error>
		 */
		byte[] result   = this.getLine();
		byte[] var_name = var.getBytes(CalypteConnectionImp.ENCODE);
		
		if(!ArraysUtil.startsWith(var_name, result)){
			byte[][] parts = ArraysUtil.split(result, 0, (byte)' ');
			return ArraysUtil.toString(parts[1]);
		}
		else{
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}

	public void processSetVarResult() throws IOException, CacheException{
		
		/*
		 * ok | <error>
		 */
		byte[] result = this.getLine();

		if(result[0] != 'o'){
			Error err = this.parseError(result);
			throw new CacheException(err.code, err.message);
		}
		
	}
	
	private Object toObject(byte[] data) throws ClassNotFoundException, IOException{
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		ObjectInputStream bin = new ObjectInputStream(in);
		return bin.readObject();
	}
	
	private Error parseError(byte[] value){
		String error   = ArraysUtil.toString(value);
		String codeSTR = error.substring(6, 10);
		String message = error.substring(12, error.length());
		return new Error(Integer.parseInt(codeSTR), message);
	}
	
	private byte[] getLine() throws IOException{
		return this.in.readLine();
	}
	
	private static class Error{
		
		public int code;
		
		public String message;

		public Error(int code, String message) {
			super();
			this.code = code;
			this.message = message;
		}
		
	}
	
}
