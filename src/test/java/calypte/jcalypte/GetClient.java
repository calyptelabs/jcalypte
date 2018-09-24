package calypte.jcalypte;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import calypte.jcalypte.CalypteConnection;
import calypte.jcalypte.CalypteConnectionPool;
import junit.framework.TestCase;

public class GetClient implements Runnable{
	
	private static final Random random = new Random();
	
	private CalypteConnectionPool pool;
	
	private AtomicInteger keyCount;
	
	private Throwable error;
	
	private int maxOperations;
	
	private CountDownLatch countDown;
	
	private String value;
	
	public GetClient(CalypteConnectionPool pool, 
			AtomicInteger keyCount, int maxOperations, CountDownLatch countDown, String value){
		this.keyCount      = keyCount;
		this.maxOperations = maxOperations;
		this.pool          = pool;
		this.value         = value;
		this.countDown     = countDown;
	}
	
	public void run(){
		for(int i=0;i<maxOperations;i++){
			CalypteConnection con = null;
			try{
				con = pool.getConnection();
				long key      = random.nextInt(keyCount.get());
				String strKey = Long.toString(key, Character.MAX_RADIX);
				String value  = (String)con.get(strKey);
				if(value != null){
					TestCase.assertEquals(strKey + ":" + this.value, value);
				}
			}
			catch(Throwable e){
				e.printStackTrace();
				error = e;
				break;
			}
			finally{
				if(con != null){
					try{
						con.close();
					}
					catch(Throwable ex){
						error = error != null? new Throwable(ex.toString(), error) : ex;
					}
				}
			}
		}
		
		countDown.countDown();
		
	}

	public Throwable getError() {
		return error;
	}
	
}
