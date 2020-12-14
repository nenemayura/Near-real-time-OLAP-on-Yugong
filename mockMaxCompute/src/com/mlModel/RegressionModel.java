package com.mlModel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.HttpURLConnection;
import java.net.URL;

import com.communication.RequestStat;

public class RegressionModel {

	public static void main(String args[]) {
		RequestStat callReq = new RequestStat(1234, 5678,
				"READ_RESPONSE", true, 1, 2, 3);
		call(callReq);
	}
	
	public static int call(RequestStat reqStat) {
		int result = 1;
		String output = "1";
		try {

			String requestString = "{'startTime':"+reqStat.getStartTime()
			+",'endTime':"+ reqStat.getEndTime()+
			",'readQ':"+ reqStat.getReadQ()+ 
			",'writeQ':"+ reqStat.getWriteQ()+ 
			",'numNodes':"+ reqStat.getNumNodes()+
			",'requestType':'"+ reqStat.getRequestType()
			+"'}";

			requestString = "http://127.0.0.1:5000/add/"+ requestString;
			System.out.println("input to server:"+ requestString);

			URL url = new URL(requestString);

			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");

			if (conn.getResponseCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

			
			System.out.println("Output from Server:");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
				result = Integer.valueOf(output);
			}

			conn.disconnect();

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
}

//String requestString = new ObjectMapper().writeValueAsString(reqStat);
//"startTime":1234,"endTime":5678,"requestType":"READ_RESPONSE","processed":true,"readQ":1,"writeQ":2,"numNodes":3
//String requestString = "startTime="+ reqStat.getStartTime()
//+ "&endTime="+ reqStat.getEndTime()
////+ "&requestType='"+ reqStat.getRequestType()+"'"
//+ "&writeQ="+ reqStat.getWriteQ()
//+ "&numNodes="+ reqStat.getNumNodes();
