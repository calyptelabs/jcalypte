package calypte.jcalypte;

public class TXCacheHelper {

	public static abstract class ConcurrentTask extends Thread{
		
		private Throwable error;
		
		private Object value;
		
		private String key;
		
		private Object value2;
		
		public ConcurrentTask(String key,
				Object value, Object value2) {
			this.value  = value;
			this.key    = key;
			this.value2 = value2;
		}

		public void run(){
			try{
				this.execute(key, value, value2);
			}
			catch(Throwable e){
				this.error = e; 
			}
		}

		protected abstract void execute(String key, Object value,
				Object value2) throws Throwable;
		
		public Throwable getError() {
			return error;
		}
		
	}
}
