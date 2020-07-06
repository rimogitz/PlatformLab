package com.hazelcast.training.vt.PlatformLab.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import static com.hazelcast.training.vt.PlatformLab.Utils.*;

public class TextMessageSimulator {
	HazelcastInstance instance;
	public TextMessageSimulator(){
		 instance = HazelcastClient.newHazelcastClient(clientConfigForExternalHazelcast());
		//JetInstance jet = Jet.bootstrappedInstance();
		//instance = jet.getHazelcastInstance();
		Thread producer = new Thread(new Runnable(){
			public void run(){
				processInput();
			}
		}); producer.start();
		
		Thread consumer = new Thread(new Runnable(){
			public void run(){
				consumeInput();
			}
		}); 
		//consumer.start();
		
	    
	}
	
	private void processInput(){
		try {
			
			IQueue<String> queue=instance.getQueue("LabSourceQueue");
			BufferedReader reader =
	                   new BufferedReader(new InputStreamReader(System.in));
			while(true){
				System.out.println("Enter Text");
		        String value = reader.readLine();
				queue.put(value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void consumeInput(){
		IQueue<String> queue=instance.getQueue("LabSourceQueue");
		while (true){
			try {
				String value = queue.take();
				System.out.println(" Message Consumed :"+value);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		TextMessageSimulator tms = new TextMessageSimulator();
	}
    

}
