package calypte.jcalypte;

import java.awt.EventQueue;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import calypte.Configuration;
import calypte.server.CalypteServer;

import calypte.jcalypte.CalypteConnectionPool;
import calypte.jcalypte.CacheException;

/**
 *
 * @author Ribeiro
 */
public class StressCacheTest extends TestCase{
    
    private static String text = 
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh" +
            "kdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjghkdjfgh kldjfghkdfjgh klsdjfghkldsjfhgksdhfg kfjgh sdkjgh";
         
	private CalypteServer server;
	
	private CalypteConnectionPool pool;
    
	@Override
	public void setUp() throws UnknownHostException, CacheException{
		Configuration config = new Configuration();
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
		
		 pool = new CalypteConnectionPool("localhost", 1044, 1, 50);
		 
	}

	@Override
	public void tearDown() throws IOException{
		this.pool.shutdown();
		this.server.stop();
		this.server = null;
		this.pool   = null;
		System.gc();
	}
	
    public void test() throws CacheException, InterruptedException {
        
		int totalClients  = 100;
		int maxOperations = 100;
		CountDownLatch countDown = new CountDownLatch(totalClients);
		AtomicInteger keyCount   = new AtomicInteger();
		Thread[] clients         = new Thread[totalClients];
		
		for(int i=0;i<totalClients;i++){
			if(i % 2 == 0){
				clients[i] = new Thread(new PutClient(pool, keyCount, maxOperations, countDown, text));
			}
			else{
				clients[i] = new Thread(new GetClient(pool, keyCount, maxOperations, countDown, text));
			}
		}
    	
		for(Thread c: clients){
			c.start();
		}

		countDown.await();
		
    }    
    
}
