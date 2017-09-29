package com.example.parser;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParserBack {
	// 3가지 필수 입력. N/A일 수 없음
	private int year;
	private int month;
	private String uniqueCarrier; // 항공사 정보

	private int arriveDelayTime;
	private int departureDelayTime;
	private int distance;

	private boolean arriveDelayAvailable = true; // null이 아닌 경우 true.
	private boolean depatureDelayAvailable = true;
	private boolean distanceAvailable = true;

	public AirlinePerformanceParserBack(Text text) { // 레코드를 필드로 변환 작업

		String[] columns = text.toString().split(",");

		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);

		uniqueCarrier = columns[8];

		// 출발지연 parsing
		if (!columns[15].equalsIgnoreCase("NA")) {
			departureDelayTime = Integer.parseInt(columns[15]);
		} else {
			depatureDelayAvailable = false;
		}

		// 도착지연 parsing
		if (!columns[14].equalsIgnoreCase("NA")) {
			arriveDelayTime = Integer.parseInt(columns[14]);
		} else {
			arriveDelayAvailable = false;
		}

		// 운항거리 parsing
		if (!columns[18].equalsIgnoreCase("NA")) {
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

	public boolean isDepatureDelayAvailable() {
		return depatureDelayAvailable;
	}

	public void setDepatureDelayAvailable(boolean depatureDelayAvailable) {
		this.depatureDelayAvailable = depatureDelayAvailable;
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
