package simpleindexing;

/*
 * Crea dos threads y el thread main espera a que acaben
 * 
 */

class PrintThread extends Thread {
	String s;

	PrintThread(String s) {
		this.s = s;
	}

	public void run() {
		for (int x = 0; x < 10; ++x) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} /* sleep 1s. to give a chance to other threads */
			System.out.print(s);
		}

	}
}

public class SimpleThread1 {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		PrintThread p1 = new PrintThread("AAAAAA");
		p1.start();
		PrintThread p2 = new PrintThread("BBBBBB");
		p2.start();

		try {
			p1.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			p2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println();
		System.out.println("Finalizado thread " + p1.getName());

		System.out.println();
		System.out.println("Finalizado thread " + p2.getName());

	}

}