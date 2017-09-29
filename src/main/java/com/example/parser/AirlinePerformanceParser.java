package com.example.parser;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {

	//필수 입력 사항
	private int year;
	private int month;
	private String uniqueCarrier;

	//응답이 없을 수 있음... --> 데이터가 없거나 소실되었을 수도 있다.
	private int arriveDelayTime;
	private int departureDelayTime;
	private int distance;

	//데이터에는 없지만 부가적으로 만든 필드
	private boolean arriveDelayAvailable = true;		//null값을 허용하지 않겠다. 값이 없으면, 통계대상에서 제외
	private boolean departureDelayAvailable = true;
	private boolean distanceAvailable = true;
	

	public AirlinePerformanceParser(Text text){		//레코드를 필드로 변환 작업하는 메소드
		String[] columns = text.toString().split(",");
		
		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);
		
		uniqueCarrier = columns[8];
		
		if(!columns[15].equals("NA")){
			departureDelayTime = Integer.parseInt(columns[15]);
		} else {
			departureDelayAvailable = false;
		}
		
		if(!columns[14].equals("NA")){
			arriveDelayTime = Integer.parseInt(columns[14]);
		} else {
			arriveDelayAvailable = false;
		}
		
		if(!columns[18].equals("NA")){
			distance = Integer.parseInt(columns[18]);
		} else {
			distanceAvailable = false;
		}
	}
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getArriveDelayTime() {
		return arriveDelayTime;
	}

	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}

	public int getDepartureDelayTime() {
		return departureDelayTime;
	}

	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}

	public void setArriveDelayAvailable(boolean arriveDelayAvailable) {
		this.arriveDelayAvailable = arriveDelayAvailable;
	}

	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}

	public void setDepartureDelayAvailable(boolean departureDelayAvailable) {
		this.departureDelayAvailable = departureDelayAvailable;
	}

	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}

	public void setDistanceAvailable(boolean distanceAvailable) {
		this.distanceAvailable = distanceAvailable;
	}

	public String getUniqueCarrier() {
		return uniqueCarrier;
	}

	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}
}
