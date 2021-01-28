package com.mad322.server;

public class Business 
{
   private String incorp_date;
   private String name;
   private String state_id;
   private int cust_id;
public String getIncorp_date() {
	return incorp_date;
}
public void setIncorp_date(String incorp_date) {
	this.incorp_date = incorp_date;
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
public String getState_id() {
	return state_id;
}
public void setState_id(String state_id) {
	this.state_id = state_id;
}
public int getCust_id() {
	return cust_id;
}
public void setCust_id(int cust_id) {
	this.cust_id = cust_id;
}
@Override
public String toString() {
	return "Business [incorp_date=" + incorp_date + ", name=" + name + ", state_id=" + state_id + ", cust_id=" + cust_id
			+ "]";
}
   
}
