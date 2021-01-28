package com.mad322.server;


public class Account {
	
	private int account_id;
	private float avail_balance;
	private String close_date;
	private String last_activity_date;
	private String open_date;
	private float pending_balance;
	private String status;
	private int cust_id;
	private int open_branch_id;
	private int open_emp_id;
	private String product_cd;
	
	public int getAccount_id() {
		return account_id;
	}
	public void setAccount_id(int account_id) {
		this.account_id = account_id;
	}
	public float getAvail_balance() {
		return avail_balance;
	}
	public void setAvail_balance(float avail_balance) {
		this.avail_balance = avail_balance;
	}
	public String getClose_date() {
		return close_date;
	}
	public void setClose_date(String close_date) {
		this.close_date = close_date;
	}
	public String getLast_activity_date() {
		return last_activity_date;
	}
	public void setLast_activity_date(String last_activity_date) {
		this.last_activity_date = last_activity_date;
	}
	public String getOpen_date() {
		return open_date;
	}
	public void setOpen_date(String open_date) {
		this.open_date = open_date;
	}
	public float getPending_balance() {
		return pending_balance;
	}
	public void setPending_balance(float pending_balance) {
		this.pending_balance = pending_balance;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public int getCust_id() {
		return cust_id;
	}
	public void setCust_id(int cust_id) {
		this.cust_id = cust_id;
	}
	public int getOpen_branch_id() {
		return open_branch_id;
	}
	public void setOpen_branch_id(int open_branch_id) {
		this.open_branch_id = open_branch_id;
	}
	public int getOpen_emp_id() {
		return open_emp_id;
	}
	public void setOpen_emp_id(int open_emp_id) {
		this.open_emp_id = open_emp_id;
	}
	public String getProduct_cd() {
		return product_cd;
	}
	public void setProduct_cd(String product_cd) {
		this.product_cd = product_cd;
	}
	@Override
	public String toString() {
		return "Account [avail_balance=" + avail_balance + ", close_date=" + close_date + ", last_activity_date="
				+ last_activity_date + ", open_date=" + open_date + ", pending_balance=" + pending_balance + ", status="
				+ status + ", cust_id=" + cust_id + ", open_branch_id=" + open_branch_id + ", open_emp_id="
				+ open_emp_id + ", product_cd=" + product_cd + ", account_id=" + account_id + "]";
	}
	
	
	

}
