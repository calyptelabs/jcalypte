package calypte.jcalypte;

import java.awt.EventQueue;
import java.io.IOException;
import java.net.UnknownHostException;

import calypte.Configuration;
import calypte.jcalypte.CalypteConnection;
import calypte.jcalypte.CalypteConnectionPool;
import calypte.jcalypte.CacheException;
import calypte.jcalypte.TXCacheHelper.ConcurrentTask;
import calypte.server.CalypteServer;
import junit.framework.TestCase;

public class TXCacheTest extends TestCase{

	private static final String SERVER_HOST	= "localhost";

	private static final int SERVER_PORT	= 1044;
	
	private static final String KEY    = "teste";

	private static final String VALUE  = "value";

	private static final String VALUE2 = "val";
	
	private CalypteConnectionPool connectionPool;
	
	private CalypteServer server;
	
	@Override
	public void setUp() throws UnknownHostException, CacheException{
		Configuration config = new Configuration();

		config.setProperty("transaction_support", "true");
		config.setProperty("max_size_key", "1024");
		
		this.server = new CalypteServer(config);
		EventQueue.invokeLater(new Runnable(){

			public void run() {
				try{
					server.start();
				}
				catch(Throwable e){
					e.printStackTrace();
				}
				
			}
			
		});
		
		//inicia o pool de conexões
		this.connectionPool = 
				new CalypteConnectionPool(SERVER_HOST, SERVER_PORT, 1, 5);
	}

	@Override
	public void tearDown() throws IOException{
		this.server.stop();
		this.server = null;
		System.gc();
	}
	
	public void testCommitFail() throws Throwable{
		CalypteConnection con = this.connectionPool.getConnection();
		try{
			con.commit();
			fail("expected CacheException");
		}
		catch(CacheException e){
			if(e.getCode() != 1013){
				fail();
			}
		}
	}
	
	public void testCommit() throws Throwable{
		String prefixKEY = "testCommit:";
		CalypteConnection con = this.connectionPool.getConnection();
		con.remove(prefixKEY + KEY);
		con.setAutoCommit(false);
		assertFalse(con.put(prefixKEY + KEY, VALUE, 0, 0));
		assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testRollbackFail() throws Throwable{
		CalypteConnection con = this.connectionPool.getConnection();
		try{
			con.rollback();
			fail("expected CacheException");
		}
		catch(CacheException e){
			if(e.getCode() != 1013){
				fail();
			}
		}
	}
	
	public void testRollback() throws Throwable{
		String prefixKEY = "testRollback:";
		CalypteConnection con = this.connectionPool.getConnection();
		con.remove(prefixKEY + KEY);
		con.setAutoCommit(false);
		assertFalse(con.put(prefixKEY + KEY, VALUE, 0, 0));
		assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.rollback();
		assertNull(con.get(prefixKEY + KEY));
	}
	
	/* replace */
	
	public void testReplace() throws Throwable{
		String prefixKEY = "testReplace:";
		CalypteConnection con = this.connectionPool.getConnection();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
	}

	public void testReplaceSuccess() throws Throwable{
		String prefixKEY = "testReplaceSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	public void testReplaceExact() throws Throwable{
		String prefixKEY = "testReplaceExact:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
	}

	public void testReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testReplaceExactSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testputIfAbsent() throws Throwable{
		String prefixKEY = "testputIfAbsent:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testputIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testputIfAbsentExistValue:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* put */
	
	public void testPut() throws Throwable{
		String prefixKEY = "testPut:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testGet() throws Throwable{
		String prefixKEY = "testGet:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testGetOverride() throws Throwable{
		String prefixKEY = "testGetOverride:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testRemoveExact() throws Throwable{
		String prefixKEY = "testRemoveExact:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}

	public void testRemove() throws Throwable{
		String prefixKEY = "testRemove:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}
	
	/* with explicit transaction */

	/* replace */
	
	public void testExplicitTransactionReplace() throws Throwable{
		String prefixKEY = "testExplicitTransactionReplace:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
		con.commit();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
	}

	public void testExplicitTransactionReplaceSuccess() throws Throwable{
		String prefixKEY = "testExplicitTransactionReplaceSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionReplaceExact() throws Throwable{
		String prefixKEY = "testExplicitTransactionReplaceExact:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		con.commit();
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
	}

	public void testExplicitTransactionReplaceExactSuccess() throws Throwable{
		String prefixKEY = "testExplicitTransactionReplaceExactSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testExplicitTransactionPutIfAbsent() throws Throwable{
		String prefixKEY = "testExplicitTransactionPutIfAbsent:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionPutIfAbsentExistValue() throws Throwable{
		String prefixKEY = "testExplicitTransactionPutIfAbsentExistValue:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* put */
	
	public void testExplicitTransactionPut() throws Throwable{
		String prefixKEY = "testExplicitTransactionPut:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testExplicitTransactionGet() throws Throwable{
		String prefixKEY = "testExplicitTransactionGet:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	public void testExplicitTransactionGetOverride() throws Throwable{
		String prefixKEY = "testExplicitTransactionGetOverride:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testExplicitTransactionRemoveExact() throws Throwable{
		String prefixKEY = "testExplicitTransactionRemoveExact:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		con.commit();
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
	}

	public void testExplicitTransactionRemove() throws Throwable{
		String prefixKEY = "testExplicitTransactionRemove:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		TestCase.assertNull(con.get(prefixKEY + KEY));
	}	
	
	/* concurrent transaction*/
	
	/* replace */
	
	public void testConcurrentTransactionReplace() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionReplace:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.get(prefixKEY + KEY, true));
		task.start();
		Thread.sleep(2000);
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, 0, 0));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionReplaceSuccess() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionReplaceSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}
	
	public void testConcurrentTransactionReplaceExact() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionReplaceExact:";
		final CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.get(prefixKEY + KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertFalse(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionReplaceExactSuccess() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionReplaceExactSuccess:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.replace(prefixKEY + KEY, VALUE, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE2, (String)con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
	}

	/* putIfAbsent */
	
	public void testConcurrentTransactionPutIfAbsent() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionPutIfAbsent:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.get(prefixKEY + KEY, true);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertNull(con.putIfAbsent(prefixKEY + KEY, VALUE, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionPutIfAbsentExistValue() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionPutIfAbsentExistValue:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.putIfAbsent(prefixKEY + KEY, VALUE2, 0, 0));
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* put */
	
	public void testConcurrentTransactionPut() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionPut:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* get */
	
	public void testConcurrentTransactionGet() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionGet:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionGetOverride() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionGetOverride:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);
		
		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE2, 0, 0);
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* remove */
	
	public void testConcurrentTransactionRemoveExact() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionRemoveExact:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE));
		con.put(prefixKEY + KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, con.get(prefixKEY + KEY));
		
		TestCase.assertFalse(con.remove(prefixKEY + KEY, VALUE2));
		TestCase.assertTrue(con.remove(prefixKEY + KEY, VALUE));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		
		if(task.getError() != null){
			throw task.getError();
		}
		
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	public void testConcurrentTransactionRemove() throws Throwable{
		final String prefixKEY = "testConcurrentTransactionRemove:";
		CalypteConnection con = this.connectionPool.getConnection();

		ConcurrentTask task = new ConcurrentTask(prefixKEY + KEY, VALUE, VALUE2){

			@Override
			protected void execute(String key, Object value,
					Object value2) throws Throwable {
				CalypteConnection con = connectionPool.getConnection();
				con.put(prefixKEY + KEY, value2, 0, 0);
			}
			
		};
		
		con.setAutoCommit(false);
		
		TestCase.assertNull((String)con.get(prefixKEY + KEY));
		TestCase.assertFalse(con.remove(prefixKEY + KEY));
		con.put(prefixKEY + KEY, VALUE, 0, 0);

		task.start();
		Thread.sleep(2000);
		
		TestCase.assertEquals(VALUE, (String)con.get(prefixKEY + KEY));
		TestCase.assertTrue(con.remove(prefixKEY + KEY));
		TestCase.assertNull(con.get(prefixKEY + KEY));
		con.commit();
		
		Thread.sleep(1000);
		if(task.getError() != null){
			throw task.getError();
		}
		TestCase.assertEquals(VALUE2, con.get(prefixKEY + KEY));
	}

	/* timeToLive */
	
	public void testTimeToLive() throws Throwable{
		final String prefixKEY = "testTimeToLive:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.put(prefixKEY + KEY, VALUE, 1000, 0);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(400);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testTimeToLiveLessThanTimeToIdle() throws Throwable{
		String prefixKEY = "testTimeToLiveLessThanTimeToIdle:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.put(prefixKEY + KEY, VALUE, 1000, 5000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testNegativeTimeToLive() throws Throwable{
		try{
			String prefixKEY = "testNegativeTimeToLive:";
			CalypteConnection con = this.connectionPool.getConnection();
			
			con.put(prefixKEY + KEY, VALUE, -1, 5000);
			fail();
		}
		catch(CacheException e){
			if(e.getCode() != 1004 || !e.getMessage().equals("Bad command syntax error!")){
				fail();
			}
				
		}
	}

	/* TimeToIdle */
	
	public void testTimeToIdle() throws Throwable{
		String prefixKEY = "testTimeToIdle:";
		CalypteConnection con = this.connectionPool.getConnection();
		
		con.put(prefixKEY + KEY, VALUE, 0, 1000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
		
	}

	public void testTimeToIdleLessThanTimeToLive() throws Throwable{
		String prefixKEY = "testTimeToIdleLessThanTimeToLive:";
		CalypteConnection con = this.connectionPool.getConnection();

		con.put(prefixKEY + KEY, VALUE, 20000, 1000);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(800);
		assertEquals(con.get(prefixKEY + KEY), VALUE);
		Thread.sleep(1200);
		assertNull(con.get(prefixKEY + KEY));
	}

	public void testNegativeTimeToIdle() throws Throwable{
		try{
			String prefixKEY = "testNegativeTimeToIdle:";
			CalypteConnection con = this.connectionPool.getConnection();
			
			con.put(prefixKEY + KEY, VALUE, 0, -1);
			fail();
		}
		catch(CacheException e){
			if(e.getCode() != 1004 || !e.getMessage().equals("Bad command syntax error!")){
				fail();
			}
				
		}
	}
	
}
