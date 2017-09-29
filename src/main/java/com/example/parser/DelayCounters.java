package com.example.parser;

public enum DelayCounters {
	/*
	 * String으로 변수 이름을 적을 때, 오타로 인한 오류 발생을 줄이고
	 *  content assist 도움을 받을 수 있어 최근에는 enum을 활용하는 추세
	 *  enum 대신 final static 사용 가능
	 */
	
	not_available_departure, 
	scheduled_departure, 
	early_departure,
	delay_departure,
	
	not_available_arrival,
	scheduled_arrival,
	early_arrival,
	delay_arrival;
}
