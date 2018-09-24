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

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author Ribeiro
 */
class BufferedOutputStream extends OutputStream{
    
    private int offset;
    
    private byte[] buffer;
    
    private int capacity;
    
    private OutputStream out;

    private boolean hasLineFeed;
    
    public BufferedOutputStream(int capacity, OutputStream out){
        
        if(capacity < 1)
            throw new IllegalArgumentException("capacity");
        
        this.offset   = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.out      = out;
    }

    public void write(byte[] buffer) throws IOException{
        this.write(buffer, 0, buffer.length);
    }

    public void write(int i) throws IOException{
        this.write(new byte[]{(byte)(i & 0xff)}, 0, 1);
    }
    
    public void write(byte[] buffer, int offset, int len) throws IOException{

    	if(len > this.capacity){
    		this.flush();
    		this.out.write(buffer, offset, len);
    		return;
    	}
    	
        int limitOffset  = offset + len;
        
        if(this.offset == this.capacity)
        	this.flush();
        
        while(offset < limitOffset){
            int maxRead  = limitOffset - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxWrite);
                offset      += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
                System.arraycopy(buffer, offset, this.buffer, this.offset, maxRead);
                offset       += maxRead;
                this.offset  += maxRead;
            }
        }
    }

    public OutputStream getDirectOutputStream(){
    	return this.out;
    }
    
    public void flush() throws IOException{
    	if(this.offset > 0){
	        this.out.write(this.buffer, 0, this.offset);
	        this.out.flush();
	        this.offset = 0;
    	}
    }

    public void close() throws IOException{
    	this.flush();
    }
    
    public void clear(){
        this.offset = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

}
