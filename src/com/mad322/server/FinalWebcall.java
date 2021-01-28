package com.mad322.server;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.json.JSONArray;
import org.json.JSONObject;



@Path("/webcall")
public class FinalWebcall
{
	
	Connection connect = null;
	Statement stmt =  null;
	ResultSet res = null;
	
	  JSONObject main = new JSONObject();
	  JSONArray  jsArray = new JSONArray();
	  JSONObject child = new JSONObject();
	
	//Displaying from customers 
  @GET
  @Path("/aggala")
  @Produces(MediaType.APPLICATION_JSON)
  public Response chandra()
  {
	  
	  
	  MysqlConnection connection = new MysqlConnection();
	   connect = connection.getConnection();
	   
	   try 
	   {
		stmt = connect.createStatement();
		res = stmt.executeQuery("select * from customer;");
		
		while(res.next())
		{
		  child = new JSONObject();
		  child.accumulate("Customer City", res.getString("CITY"));	
		  child.accumulate("Customer ID", res.getString("CUST_ID"));
		  child.accumulate("Customer State", res.getString("STATE"));
		
		 		  
		  jsArray.put(child);
		}
		
		main.put("employee", jsArray);
		
	   }catch(SQLException e)
		{
			System.out.println("SQL Exception : +e.getMessage");
		}
		finally
		{
			try
			{
				connect.close();
				stmt.close();
				res.close();
			}
			catch (SQLException e)
			{
				System.out.println("Finally Block SQL Exception : "+e.getMessage());
			}
			
		}
	
	return Response.status(200).entity(main.toString()).build();
	  	  

  }
  

  PreparedStatement preparStatement = null;
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/editEmp/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateEmployee(@PathParam("id")int id,Employee emp)
  {
  	MysqlConnection connection= new MysqlConnection();
  	connect= connection.getConnection();
  	Status status =Status.OK;
  	try 
  	{
  		String query ="UPDATE `employee` SET `EMP_ID` =?,`END_DATE` =?,`FIRST_NAME` =?,`LAST_NAME` =?, `START_DATE` =?,`TITLE` =?,`ASSIGNED_BRANCH_ID` =?,`DEPT_ID` =?,`SUPERIOR_EMP_ID` =? WHERE `EMP_ID` ="+id;
  		
  		preparStatement = connect.prepareStatement(query);   		 		   		
  		preparStatement.setInt(1, emp.geteMP_ID());
  		preparStatement.setString(2, emp.geteND_DATE());
  		preparStatement.setString(3,emp.getfIRST_NAME());
  		preparStatement.setString(4,emp.getlAST_NAME());
  		preparStatement.setString(5,emp.getsTART_DATE());
  		preparStatement.setString(6, emp.gettITLE());
  		preparStatement.setInt(7, emp.getaSSIGNED_BRANCH_ID());  
  		preparStatement.setInt(8, emp.getdEPT_ID());
  		preparStatement.setInt(9, emp.getsUPERIOR_EMP_ID());
  		
  		int rowCount = preparStatement.executeUpdate(); 		
  		if (rowCount > 0) 
  		{
  		status=Status.OK;
  		main.accumulate("status", status);
  		main.accumulate("Message","Data successfully updated !");		
  		}
  		else
  		{
  			status=Status.NOT_MODIFIED;
  			main.accumulate("status", status);
  			main.accumulate("Message","Something Went Wrong");						
  		}  		
  	}
  	catch(SQLException e)
  	{
  		e.printStackTrace();
  		status=Status.NOT_MODIFIED;
  		main.accumulate("status", status);
  		main.accumulate("Message","Something Went Wrong");
  	}  	
  	return Response.status(status).entity(main.toString()).build();
  }	
  
  PreparedStatement prepareStatement = null;
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/editCusto/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateCustomer(@PathParam("id")int id,Customer custo)
  {
  	MysqlConnection connection= new MysqlConnection();
  	connect= connection.getConnection();
  	Status status =Status.OK;
  	try 
  	{
  		String query ="UPDATE `customer` SET `CUST_ID` =?,`ADDRESS` =?,`CITY` =?,`CUST_TYPE_CD` =?, `FED_ID` =?,`POSTAL_CODE` =?,`STATE` =? WHERE `CUST_ID` ="+id;
  		
  		prepareStatement = connect.prepareStatement(query);   		 		   		
  		prepareStatement.setInt(1, custo.getCUST_ID());
  		prepareStatement.setString(2, custo.getADDRESS());
  		prepareStatement.setString(3,custo.getCITY());
  		prepareStatement.setString(4,custo.getCUST_TYPE_CD());
  		prepareStatement.setString(5,custo.getFED_ID());
  		prepareStatement.setString(6, custo.getPOSTAL_CODE());
  		prepareStatement.setString(7, custo.getSTATE());  
  		
  		
  		int rowCount = prepareStatement.executeUpdate(); 		
  		if (rowCount > 0) 
  		{
  		status=Status.OK;
  		main.accumulate("status", status);
  		main.accumulate("Message","Data successfully updated !");		
  		}
  		else
  		{
  			status=Status.NOT_MODIFIED;
  			main.accumulate("status", status);
  			main.accumulate("Message","Something Went Wrong");						
  		}  
  		
  	}
  	catch(SQLException e)
  	{
  		e.printStackTrace();
  		status=Status.NOT_MODIFIED;
  		main.accumulate("status", status);
  		main.accumulate("Message","Something Went Wrong");
  	}  	
  	return Response.status(status).entity(main.toString()).build();
  }	
 
//Displaying from employees whose department id is given from parameters
  @GET
  @Path("/teja/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response teja(@PathParam("id")int id)
  {
	  
	  
	  MysqlConnection connection = new MysqlConnection();
	   connect = connection.getConnection();
	   
	   try 
	   {
		stmt = connect.createStatement();
		res = stmt.executeQuery("select * from employee where DEPT_ID = "+id+";");
		
		while(res.next())
		{
		  child = new JSONObject();
		  
		  
		  child.accumulate("Employee Name", res.getString("FIRST_NAME"));	
		  child.accumulate("Department ID", res.getString("DEPT_ID"));
		
		 		  
		  jsArray.put(child);
		}
		
		main.put("employee", jsArray);
		
	   }catch(SQLException e)
		{
			System.out.println("SQL Exception : +e.getMessage");
		}
		finally
		{
			try
			{
				connect.close();
				stmt.close();
				res.close();
			}
			catch (SQLException e)
			{
				System.out.println("Finally Block SQL Exception : "+e.getMessage());
			}
			
		}
	
	return Response.status(200).entity(main.toString()).build();
	  	  

  }
  
	// Displaying from Employees whose assigned branch is given in the parameter
  @GET
  @Path("/sasank/{t}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sasank(@PathParam("t")int t)
  {
	  
	  
	  MysqlConnection connection = new MysqlConnection();
	 connect = connection.getConnection();
	   
	   try 
	   {
		stmt = connect.createStatement();
		res = stmt.executeQuery("select * from employee where ASSIGNED_BRANCH_ID = "+t+";");
		
		while(res.next())
		{
		  child = new JSONObject();
		  
		  
		  child.accumulate("Employee Name", res.getString("FIRST_NAME"));	
		  child.accumulate("Department ID", res.getString("DEPT_ID"));
	
		  	 		  
		  jsArray.put(child);
		}
		
		main.put("employee", jsArray);
		
	   }catch(SQLException e)
		{
			System.out.println("SQL Exception : +e.getMessage");
		}
		finally
		{
			try
			{
				connect.close();
				stmt.close();
				res.close();
			}
			catch (SQLException e)
			{
				System.out.println("Finally Block SQL Exception : "+e.getMessage());
			}
			
		}
	
	return Response.status(200).entity(main.toString()).build();
	  	  

  }
  
  
 // INserting into Branch
  
  
  PreparedStatement prparedStatement = null;
    @POST
	@Path("/newBranch")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createBranch(Branch branch)
	{
		MysqlConnection connection = new MysqlConnection();		
		connect = connection.getConnection();
		
	try
	{	
		String query = "INSERT INTO `midterm`.`branch`(`BRANCH_ID`,`ADDRESS`,`CITY`,`NAME`,`STATE`,`ZIP_CODE`)"
				+ "VALUES(?,?,?,?,?,?)";
		
		prparedStatement = connect.prepareStatement(query);		
		prparedStatement.setInt(1, branch.getBRANCH_ID());
		prparedStatement.setString(2, branch.getADDRESS());
		prparedStatement.setString(3,branch.getCITY());
		prparedStatement.setString(4,branch.getNAME());
		prparedStatement.setString(5,branch.getSTATE());
		prparedStatement.setString(6, branch.getZIP_CODE());		
					
		int rowCount = prparedStatement.executeUpdate();
		
		if(rowCount>0)
		{
			System.out.println("Record inserted Successfully! : "+rowCount);
			
			main.accumulate("Status", 201);
			main.accumulate("Message", "Record Successfully added!");
		}
		
		
	}
	     catch (SQLException e) {

		main.accumulate("Status", 500);
		main.accumulate("Message", e.getMessage());
	}
	     finally {
		  try
		  {
			 connect.close();
		    	preparedStatement.close();
		  }
		       catch (SQLException e) {
		    	System.out.println("Finally SQL Exception : "+e.getMessage());
		  }
	}		
	return Response.status(201).entity(main.toString()).build();
					
	}
  
  
  
  
  
  
  
  
  
  
  // INserting into Customer
  
  
  PreparedStatement preparedStatement = null;
    @POST
	@Path("/newCusto")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createCustomer(Customer customer)
	{
		MysqlConnection connection = new MysqlConnection();		
		connect = connection.getConnection();
		
	try
	{	
		String query = "INSERT INTO `midterm`.`customer`(`CUST_ID`,`ADDRESS`,`CITY`,`CUST_TYPE_CD`,`FED_ID`,`POSTAL_CODE`,`STATE`)"
				+ "VALUES(?,?,?,?,?,?,?)";
		
		preparedStatement = connect.prepareStatement(query);		
		preparedStatement.setInt(1, customer.getCUST_ID());
		preparedStatement.setString(2, customer.getADDRESS());
		preparedStatement.setString(3,customer.getCITY());
		preparedStatement.setString(4,customer.getCUST_TYPE_CD());
		preparedStatement.setString(5,customer.getFED_ID());
		preparedStatement.setString(6, customer.getPOSTAL_CODE());
		preparedStatement.setString(7, customer.getSTATE());
					
		int rowCount = preparedStatement.executeUpdate();
		
		if(rowCount>0)
		{
			System.out.println("Record inserted Successfully! : "+rowCount);
			
			main.accumulate("Status", 201);
			main.accumulate("Message", "Record Successfully added!");
		}else
		{
			main.accumulate("Status", 500);
			main.accumulate("Message", "Something went wrong!");
		}
		
		
	}
	     catch (SQLException e) {

		main.accumulate("Status", 500);
		main.accumulate("Message", e.getMessage());
	}
	     finally {
		  try
		  {
			 connect.close();
		    	preparedStatement.close();
		  }
		       catch (SQLException e) {
		    	System.out.println("Finally SQL Exception : "+e.getMessage());
		  }
	}		
	return Response.status(201).entity(main.toString()).build();
					
	}
    
    
    // Inserting into Employee
    
    PreparedStatement prepaStatement = null;
    @POST
	@Path("/newEmployee")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createEmployee(Employee employee)
	{
		MysqlConnection connection = new MysqlConnection();		
		connect = connection.getConnection();
		
	try
	{	
		String query = "INSERT INTO `midterm`.`employee`(`EMP_ID`,`END_DATE`,`FIRST_NAME`,`LAST_NAME`,`START_DATE`,`TITLE`,`ASSIGNED_BRANCH_ID`,`DEPT_ID`,`SUPERIOR_EMP_ID`)"
				+ "VALUES(?,?,?,?,?,?,?,?,?)";
		
		preparedStatement = connect.prepareStatement(query);		
		preparedStatement.setInt(1, employee.geteMP_ID());
		preparedStatement.setString(2, employee.geteND_DATE());
		preparedStatement.setString(3,employee.getfIRST_NAME());
		preparedStatement.setString(4,employee.getlAST_NAME());
		preparedStatement.setString(5,employee.getsTART_DATE());
		preparedStatement.setString(6, employee.gettITLE());
		preparedStatement.setInt(7, employee.getaSSIGNED_BRANCH_ID());
		preparedStatement.setInt(8, employee.getdEPT_ID());
		preparedStatement.setInt(9, employee.getsUPERIOR_EMP_ID());
					
		int rowCount = preparedStatement.executeUpdate();
		
		if(rowCount>0)
		{
			System.out.println("Record inserted Successfully! : "+rowCount);
			
			main.accumulate("Status", 201);
			main.accumulate("Message", "Record Successfully added!");
		}else
		{
			main.accumulate("Status", 500);
			main.accumulate("Message", "Something went wrong!");
		}
		
		
	}
	     catch (SQLException e) {

		main.accumulate("Status", 500);
		main.accumulate("Message", e.getMessage());
	}
	     finally {
		  try
		  {
			 connect.close();
		    	preparedStatement.close();
		  }
		       catch (SQLException e) {
		    	System.out.println("Finally SQL Exception : "+e.getMessage());
		  }
	}		
	return Response.status(201).entity(main.toString()).build();
					
	}

    @GET
	@Path("/getAccount/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAccount(@PathParam("id") String id) {
    	MysqlConnection connection = new MysqlConnection();		
		connect = connection.getConnection();
		try {
			stmt = connect.createStatement();

			res = stmt.executeQuery("Select * from account where pending_balance<"+id);

			while (res.next()) {
				child = new JSONObject();

				child.accumulate("accountid", res.getString("account_id"));
				child.accumulate("availablebalance", res.getString("avail_balance"));
				child.accumulate("closedate", res.getDate("close_date"));
				child.accumulate("lastactivity", res.getDate("last_activity_date"));
				child.accumulate("opendate",res.getDate("open_date"));
				child.accumulate("pendingbalance",res.getString("pending_balance"));
				child.accumulate("status",res.getString("status"));
				child.accumulate("custid",res.getString("cust_id"));
				child.accumulate("openbranchid",res.getString("open_branch_id"));
				child.accumulate("openempid",res.getString("open_emp_id"));
				child.accumulate("productcd",res.getString("product_cd"));
				jsArray.put(child);
			}

			main.put("Account", jsArray);
		} catch (SQLException e) {
			System.out.println("SQL Exception : " + e.getMessage());
		} finally {
			try {
				connect.close();
				stmt.close();
				res.close();
			} catch (SQLException e) {
				System.out.println("Finally Block SQL Exception : " + e.getMessage());
			}
		}

		return Response.status(200).entity(main.toString()).build();

	}

PreparedStatement prepareStatement = null;
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/editCusto/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateCustomer(@PathParam("id")int id,Customer custo)
  {
  	MysqlConnection connection= new MysqlConnection();
  	connect= connection.getConnection();
  	Status status =Status.OK;
  	try 
  	{
  		String query ="UPDATE `customer` SET `CUST_ID` =?,`ADDRESS` =?,`CITY` =?,`CUST_TYPE_CD` =?, `FED_ID` =?,`POSTAL_CODE` =?,`STATE` =? WHERE `CUST_ID` ="+id;
  		
  		prepareStatement = connect.prepareStatement(query);   		 		   		
  		prepareStatement.setInt(1, custo.getCUST_ID());
  		prepareStatement.setString(2, custo.getADDRESS());
  		prepareStatement.setString(3,custo.getCITY());
  		prepareStatement.setString(4,custo.getCUST_TYPE_CD());
  		prepareStatement.setString(5,custo.getFED_ID());
  		prepareStatement.setString(6, custo.getPOSTAL_CODE());
  		prepareStatement.setString(7, custo.getSTATE());  
  		
  		
  		int rowCount = prepareStatement.executeUpdate(); 		
  		if (rowCount > 0) 
  		{
  		status=Status.OK;
  		main.accumulate("status", status);
  		main.accumulate("Message","Data successfully updated !");		
  		}
  		else
  		{
  			status=Status.NOT_MODIFIED;
  			main.accumulate("status", status);
  			main.accumulate("Message","Something Went Wrong");						
  		}  
  		
  	}
  	catch(SQLException e)
  	{
  		e.printStackTrace();
  		status=Status.NOT_MODIFIED;
  		main.accumulate("status", status);
  		main.accumulate("Message","Something Went Wrong");
  	}  	
  	return Response.status(status).entity(main.toString()).build();
  }	
 
//Displaying from employees whose department id is given from parameters
  @GET
  @Path("/teja/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response teja(@PathParam("id")int id)
  {
	  
	  
	  MysqlConnection connection = new MysqlConnection();
	   connect = connection.getConnection();
	   
	   try 
	   {
		stmt = connect.createStatement();
		res = stmt.executeQuery("select * from employee where DEPT_ID = "+id+";");
		
		while(res.next())
		{
		  child = new JSONObject();
		  
		  
		  child.accumulate("Employee Name", res.getString("FIRST_NAME"));	
		  child.accumulate("Department ID", res.getString("DEPT_ID"));
		
		 		  
		  jsArray.put(child);
		}
		
		main.put("employee", jsArray);
		
	   }catch(SQLException e)
		{
			System.out.println("SQL Exception : +e.getMessage");
		}
		finally
		{
			try
			{
				connect.close();
				stmt.close();
				res.close();
			}
			catch (SQLException e)
			{
				System.out.println("Finally Block SQL Exception : "+e.getMessage());
			}
			
		}
	
	return Response.status(200).entity(main.toString()).build();
	  	  

  }
	
}
