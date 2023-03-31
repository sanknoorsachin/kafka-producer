package com.example.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


import org.apache.kafka.common.serialization.StringSerializer;

import com.example.kafka.model.Address;
import com.example.kafka.model.UserModel;
import org.springframework.kafka.support.serializer.JsonSerializer;

import org.apache.kafka.clients.producer.*;



public class Producer {

	public static void main(String[] args) {
		Properties prop = new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				  StringSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,JsonSerializer.class.getName()); 
		prop.setProperty(JsonSerializer.ADD_TYPE_INFO_HEADERS, "false"); 
		
	
		UserModel model= new UserModel();
		model.setName("sachin");
		model.setAge("38");
		model.setEmployeeid("344490");
		
		Address address= new Address();
		address.setCity("Bangalore");		
		address.setHouseNumer("122");
		address.setStreet("Panathur");
		
		Address address1= new Address();
		address1.setCity("Bombay");		
		address1.setHouseNumer("222");
		address1.setStreet("st jhon");
	 
		List<Address> addList = new ArrayList<Address>();
		addList.add(address);
		addList.add(address1);
		
		model.setAddress(addList);
		
		final KafkaProducer<String , UserModel> producer = new KafkaProducer<String ,UserModel>(prop);
		ProducerRecord< String , UserModel> record = new ProducerRecord<String, UserModel>("test",model);
		
		producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				
				if(e==null) {
					
				}
			}
		});
		producer.flush();
		producer.close();
	}
	

}
