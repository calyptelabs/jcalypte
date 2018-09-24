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

/**
 * Exceção lançada quando ocorre uma falha ao interagir com o cache.
 * 
 * @author Ribeiro
 */
public class CacheException extends Exception{
    
	private static final long serialVersionUID = -2125449136205991256L;

	private int code;

    public CacheException(int code, String string, Throwable thrwbl) {
        super(string, thrwbl);
        this.code = code;
    }

    public CacheException(int code, String string) {
    	this(code, string, null);
    }
	
    public CacheException(Throwable thrwbl) {
        this(2000, "client error", thrwbl);
    }

    public CacheException(String string, Throwable thrwbl) {
    	this(2000, "client error: " + string, thrwbl);
    }
    
    public int getCode() {
		return code;
	}
    
}
