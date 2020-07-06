package com.hazelcast.training.vt.PlatformLab.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.collection.IQueue;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class MapEntrySimulator {
	HazelcastInstance instance;
	public MapEntrySimulator(){
		instance = HazelcastClient.newHazelcastClient(clientConfigForExternalHazelcast());
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
			
			IMap<String,String> map=instance.getMap("LabSourceMap");
			int i=0;
			 while(true){
				System.out.println("Adding Entry "+ (++i));
		        String value = map.put(Integer.toString(i),Double.toString(Math.random()));
				Thread.sleep(350);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void consumeInput(){
		IQueue<String> queue=instance.getQueue("Lab1SourceQueue");
		while (true){
			try {
				String value = queue.take();
				System.out.println(" Message Consumed :"+value);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MapEntrySimulator tms = new MapEntrySimulator();
	}
    
	private  ClientConfig clientConfigForExternalHazelcast() {
        ClientConfig cfg = new ClientConfig();
        cfg.getNetworkConfig().addAddress("127.0.0.1:5701");
        cfg.setClusterName("PRIMARY");
        return cfg;
    }
}
