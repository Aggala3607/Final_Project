package com.mad322.server;

public class Branch {
		private int BRANCH_ID;
		private String  ADDRESS;
		private String  CITY;
		private String  NAME;
		private String  STATE;
		private String  ZIP_CODE;
		
		

		public String getZIP_CODE() {
			return ZIP_CODE;
		}



		public void setZIP_CODE(String zIP_CODE) {
			ZIP_CODE = zIP_CODE;
		}



		public int getBRANCH_ID() {
			return BRANCH_ID;
		}



		public void setBRANCH_ID(int bRANCH_ID) {
			BRANCH_ID = bRANCH_ID;
		}



		public String getADDRESS() {
			return ADDRESS;
		}



		public void setADDRESS(String aDDRESS) {
			ADDRESS = aDDRESS;
		}



		public String getCITY() {
			return CITY;
		}



		public void setCITY(String cITY) {
			CITY = cITY;
		}



		public String getNAME() {
			return NAME;
		}



		public void setNAME(String nAME) {
			NAME = nAME;
		}



		public String getSTATE() {
			return STATE;
		}



		public void setSTATE(String sTATE) {
			STATE = sTATE;
		}



		@Override
		public String toString() {
			return "Branch [BRANCH_ID=" + BRANCH_ID + ", ADDRESS=" + ADDRESS + ", CITY=" + CITY + ", NAME=" + NAME
					+ ", STATE=" + STATE + ", ZIP_CODE=" + ZIP_CODE + "]";
		}



		


		
		
		

}
