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
import java.io.InputStream;
import java.util.Arrays;

/**
 *
 * @author Ribeiro
 */
class BufferedInputStream extends InputStream{
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private int capacity;
    
    private InputStream stream;

    public BufferedInputStream(int capacity, InputStream stream){
        this.offset   = 0;
        this.limit    = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.stream   = stream;
    }

    public int read() throws IOException{
    	
        if(this.checkBuffer() < 0){
        	return -1;
        }
    	
        return this.buffer[this.offset++];
    }
    
    public int read(byte[] b, int off, int len) throws IOException{
    	
    	int read  = 0;
    	
    	while(len > 0){
    		
            if(this.checkBuffer() < 0){
            	return read;
            }
            	
            int maxRead = this.limit - this.offset;
            
            if(len > maxRead){
            	System.arraycopy(this.buffer, this.offset, b, off, maxRead);
            	this.offset += maxRead;
            	off         += maxRead;
            	read        += maxRead;
            	len         -= maxRead;
            }
            else{
            	System.arraycopy(this.buffer, this.offset, b, off, len);
            	this.offset += len;
            	read        += len;
            	return read; 
            }
            
    	}
    	
    	return read;
    }
    
    private int checkBuffer() throws IOException{
    	
        if(this.offset == this.limit){
            
            if(this.limit == this.capacity){
                this.offset = 0;
                this.limit  = 0;
            }
            
            int len = stream.read(this.buffer, this.limit, this.buffer.length - limit);
            
            if(len == -1){
            	return -1;
            }
                //throw new EOFException("premature end of data");
            
            this.limit += len;
            return len;
        }    		
    	
        return 0;
    }
    
    public int readFullLineInBytes(byte[] b, int off, int len) throws IOException{
    	
    	int startOff = this.offset;
    	int read     = 0;

		int maxRead;
		int maxWrite;
		int transf;
    	
    	for(;;){
    		maxRead  = this.offset - startOff;
    		maxWrite = len;
    		transf   = maxRead > maxWrite? maxWrite : maxRead;

    		if(maxWrite == 0){
    			throw new IOException("out of memoty");
    		}
    		
            if(this.offset == this.limit){
            	System.arraycopy(this.buffer, startOff, b, off, transf);
            	
            	len -= transf;
            	off += transf;
            	read+= transf;
            	
            	if(this.checkBuffer() < 0)
            		return read;
            	
            	startOff = this.offset;
            }
            
            if(this.buffer[this.offset++] == '\n'){
            	System.arraycopy(this.buffer, startOff, b, off, transf);
            	read+= transf;
            	return read;
            }
            
    	}
    	
    }
    
    public byte[] readLine() throws IOException{
    	
    	ByteArrayOutputStream bout = new ByteArrayOutputStream(64);
    	int startOff  = this.offset;
    	
    	for(;;){
            if(this.offset == this.limit){
            	bout.write(this.buffer, startOff, this.offset - startOff);
            	if(this.checkBuffer() < 0)
            		return bout.toByteArray();
            	startOff = this.offset;
            }
            
            if(this.buffer[this.offset++] == '\n'){
            	bout.write(this.buffer, startOff, this.offset - startOff);
            	byte[] array = bout.toByteArray();
            	
            	if(array.length > 1){ 
            		if(array[array.length-2] == '\r'){
            			return Arrays.copyOf(array, array.length - 2);
            		}
            		else{
                        throw new IOException("expected \\r");
            		}
            	}
            	else{
            		return array;
            	}
            }
            
    	}
    	
    }
    
    public void clear(){
        this.offset = 0;
        this.limit  = 0;
    }

}
