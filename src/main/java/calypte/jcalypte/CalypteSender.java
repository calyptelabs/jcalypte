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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

class CalypteSender {
	
    private OutputStream out;
    
    public CalypteSender(Socket socket, StreamFactory streamFactory, 
    		int bufferLength) throws IOException{
    	this.out = 
    			new BufferedOutputStream(bufferLength, streamFactory.createOutputStream(socket));
    }
    
	public void executePut(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 put <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
		 */
		
		byte[] data = this.toBytes(value);
		
		out.write(CalypteConnectionImp.PUT_COMMAND_DTA);
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(ArraysUtil.toBytes(key));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(ArraysUtil.toBytes(timeToLive));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(ArraysUtil.toBytes(timeToIdle));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(ArraysUtil.toBytes(data.length));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(CalypteConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
		
	}
	
	public void executeReplace(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 put <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
			 end\r\n 
		 */

		byte[] data = this.toBytes(value);
		
		out.write(CalypteConnectionImp.REPLACE_COMMAND_DTA);
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToLive).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToIdle).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(Integer.toString(data.length).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(CalypteConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeSet(String key, long timeToLive, long timeToIdle, Object value) throws IOException {
		
		/*
			 set <key> <timeToLive> <timeToIdle> <size> <reserved>\r\n
			 <data>\r\n
			 end\r\n 
		 */

		byte[] data = this.toBytes(value);
		
		out.write(CalypteConnectionImp.SET_COMMAND_DTA);
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToLive).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);

		out.write(Long.toString(timeToIdle).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(Integer.toString(data.length).getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(CalypteConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.write(data, 0, data.length);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeGet(String key, boolean forUpdate) throws IOException{
		/*
			get <key> <update> <reserved>\r\n
		 */
		
		out.write(CalypteConnectionImp.GET_COMMAND_DTA);
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(forUpdate? '1' : '0');
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(CalypteConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeRemove(String key) throws IOException{
		
		/*
			delete <name> <reserved>\r\n
		 */
		
		out.write(CalypteConnectionImp.REMOVE_COMMAND_DTA);	
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(key.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(CalypteConnectionImp.DEFAULT_FLAGS_DTA);
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeBeginTransaction() throws IOException{
		
		/*
			begin\r\n
		 */
		
		out.write(CalypteConnectionImp.BEGIN_TX_COMMAND_DTA);	
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeCommitTransaction() throws IOException{
		
		/*
			commit\r\n
		 */
		
		out.write(CalypteConnectionImp.COMMIT_TX_COMMAND_DTA);	
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeRollbackTransaction() throws IOException{
		
		/*
			rollback\r\n
		 */
		
		out.write(CalypteConnectionImp.ROLLBACK_TX_COMMAND_DTA);	
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	public void executeShowVar(String var) throws IOException{
		
		/*
			show_var <var_name>\r\n
		 */
		
		out.write(CalypteConnectionImp.SHOW_VAR);	
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(var.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}

	public void executeSetVar(String var, Object value) throws IOException{
		
		/*
			set_var <var_name> <var_value>\r\n
		 */
		
		String strValue = String.valueOf(value);
		
		out.write(CalypteConnectionImp.SET_VAR);	
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(var.getBytes(CalypteConnectionImp.ENCODE));
		out.write(CalypteConnectionImp.SEPARATOR_COMMAND_DTA);
		
		out.write(strValue.getBytes(CalypteConnectionImp.ENCODE));	
		out.write(CalypteConnectionImp.CRLF_DTA);
		
		out.flush();
	}
	
	private byte[] toBytes(Object value) throws IOException{
        ObjectOutputStream out     = null;
    	ByteArrayOutputStream bout = null;
    	
        try{
            bout = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bout);
            out.writeObject(value);
            out.flush();
            return bout.toByteArray();
        }
        finally{
            try{
                if(out != null){
                    out.close();
                }
            }
            catch(Exception ex){
                ex.printStackTrace();
            }
        }
		
	}
}
