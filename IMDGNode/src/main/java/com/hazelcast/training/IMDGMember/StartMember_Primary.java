package com.hazelcast.training.IMDGMember;

import java.io.FileNotFoundException;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;

/**
 * Hello world!
 *
 */
public class StartMember_Primary 
{
    public static void main( String[] args )
    {
        System.out.println( "Starting Hazelcast" );
        try {
			Hazelcast.newHazelcastInstance(new XmlConfigBuilder("src/main/resources/hazelcast_PRIMARY.xml").build());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
