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
 * Permite o armazenamento, atualização, remoção de um item no Calypte.
 * 
 * @author Ribeiro.
 */
public interface CalypteConnection {
    
    /*
     * Faz a conexão com o servidor.
     * 
     * @throws CacheException Lançada caso ocorra alguma falha ao tentar se
     * conectar ao servidor.
     */
    //void connect() throws CacheException;
    
    /**
     * Fecha a conexão com o servidor.
     * 
     * @throws CacheException Lançada caso ocorra alguma falha ao tentar fechar a conexão com o servidor.
     */
    void close() throws CacheException;
    
    /**
     * Verifica se a conexão está fechada.
     * @return <code>true</code> se a conexão está fechada. Caso contrátio, <code>false</code>.
     */
    boolean isClosed();
    
	/* métodos de coleta*/
    
    /**
     * Substitui o valor associado à chave somente se ele existir.
     * @param key chave associada ao valor.
     * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
     */
	boolean replace(
			String key, Object value, long timeToLive, long timeToIdle) throws CacheException;
    
	/**
	 * Substitui o valor associado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @param oldValue valor esperado associado à chave.
	 * @param newValue valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return <code>true</code> se o valor for substituido. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
	boolean replace(
			String key, Object oldValue, 
			Object newValue, long timeToLive, long timeToIdle) throws CacheException;
	
	/**
	 * Associa o valor à chave somente se a chave não estiver associada a um valor.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return valor anterior associado à chave.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
	Object putIfAbsent(
			String key, Object value, long timeToLive, long timeToIdle) throws CacheException;

	/**
	 * Associa o valor à chave somente se a chave não estiver associada a um valor.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
	 * @return <code>true</code> se o valor for substituído. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
	boolean set(
			String key, Object value, long timeToLive, long timeToIdle) throws CacheException;
	
	/**
	 * Associa o valor à chave.
	 * @param key chave associada ao valor.
	 * @param value valor para ser associado à chave.
	 * @param timeToLive é a quantidade máxima de tempo que um item expira após sua criação.
	 * @param timeToIdle é a quantidade máxima de tempo que um item expira após o último acesso.
     * @return <code>true</code> se o item for substituido. Caso contrário, <code>false</code>
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
    boolean put(String key, Object value, long timeToLive, long timeToIdle) throws CacheException;
    
	/**
     * Obtém o valor associado à chave bloqueando ou não 
     * seu acesso as demais transações.
     * @param key chave associada ao valor.
     * @param forUpdate <code>true</code> para bloquear o item. Caso contrário <code>false</code>.
     * @return valor associado à chave ou <code>null</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
    Object get(String key, boolean forUpdate) throws CacheException;

	/**
     * Obtém o valor associado à chave.
     * @param key chave associada ao valor.
     * @return valor associado à chave ou <code>null</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
    Object get(String key) throws CacheException;
    
    /* métodos de remoção */
    
	/**
	 * Remove o valor assoiado à chave somente se ele for igual a um determinado valor.
	 * @param key chave associada ao valor.
	 * @param value valor associado à chave.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
	boolean remove(
			String key, Object value) throws CacheException;
    
	/**
	 * Remove o valor associado à chave.
	 * @param key chave associada ao valor.
	 * @return <code>true</code> se o valor for removido. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor.
	 */
    boolean remove(String key) throws CacheException;
    
    /**
     * Define o modo de confirmação automática. Se o modo de confirmação automática
     * estiver ligado, todas as operações serão tratadas como transações individuais. Caso contrário,
     * as operações serão agrupadas em uma transação que deve ser confirmada com o método {@link #commit()} ou
     * descartadas com o método {@link #rollback()}. Por padrão, cada nova conexão inicia com o 
     * modo de confirmação automática ligada. 
     * @param value <code>true</code> para ligar o modo de confirmação automática. Caso contrário, <code>false</code>. 
     * @throws CacheException Lançada se o estado desejado já estiver em vigor ou se a conexão estiver fechada.
     */
    void setAutoCommit(boolean value) throws CacheException;
    
    /**
     * Obtém o estado atual do modo de confirmação automática.
     * @return <code>true</code> se o modo de confirmação automática estiver ligado. Caso contrário, <code>false</code>.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor ou se a conexão estiver fechada.
     */
    boolean isAutoCommit() throws CacheException;
    
    /**
     * Confirma todas as operações da transação atual e libera todos os bloqueios detidos por essa conexão.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor, se a conexão estiver fechada ou se o
     * modo de confirmação automática estiver ligada.
     */
    void commit() throws CacheException;
    
    /**
     * Desfaz todas as operações da transação atual e libera todos os bloqueios detidos por essa conexão.
     * @throws CacheException Lançada se ocorrer alguma falha com o servidor, se a conexão estiver fechada ou se o
     * modo de confirmação automática estiver ligada.
     */
    void rollback() throws CacheException;

    /**
     * Limpa o cache.
     * @throws CacheException Lançado se ocorrer alguma falha ao tentar limpar o cache.
     */
    void flush() throws CacheException;
    
    /**
     * Obtém o endereço do servidor.
     * @return Endereço do servidor.
     */
    String getHost();

    /**
     * Obtém a porta do servidor.
     * @return Porta do servidor.
     */
    int getPort();
    
}
