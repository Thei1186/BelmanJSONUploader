/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package belmanjsonuploader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Timer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * This project is for reading JSON files and uploading the information to the
 * database for the Belman project
 * @author Theis
 */
public class BelmanJSONUploader
{

    private DbConnectionProvider ds;
    
    public BelmanJSONUploader() throws IOException
    {
        ds = new DbConnectionProvider();
    }

    /**
     * @param args the command line arguments
     * @throws java.io.FileNotFoundException
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws FileNotFoundException, SQLException, IOException
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        BelmanJSONUploader jsonupload = new BelmanJSONUploader();
        
        Runnable task = () ->
        {
            try
            {
                jsonupload.checkJSONFolder(new File("JSON"));
            } catch (IOException | ParseException | SQLException ex)
            {
                Logger.getLogger(BelmanJSONUploader.class.getName()).log(Level.SEVERE, null, ex);
            }
        };
        
        executor.scheduleWithFixedDelay(task, 0, 30, TimeUnit.SECONDS);
    }
    
    public void uploadJSON(String path) throws FileNotFoundException, IOException, ParseException, SQLException
    {
        Object obj = new JSONParser().parse(new FileReader(path));
        
        JSONObject jObject = (JSONObject) obj;
        
        try (Connection con = ds.getConnection())
        {
            int id = 0;
            JSONArray jArray = (JSONArray) jObject.get("ProductionOrders");
            for (Object object : jArray) //Iterates over an array of production orders
            {
                JSONObject oObject = (JSONObject) object;
                
                JSONObject pOObject = (JSONObject) oObject.get("Customer");
                String customerName = (String) pOObject.get("Name");
                
                JSONObject orderObject = (JSONObject) oObject.get("Order");
                String orderNumber = (String) orderObject.get("OrderNumber");
                
                JSONObject deliveryObject = (JSONObject) oObject.get("Delivery");
                String deliveryDateString = (String) deliveryObject.get("DeliveryTime");
                Date deliveryDate = formatDateString(deliveryDateString);
                
                if (!checkIfOrderExists(con, orderNumber))
                {
                    id = uploadPOrderToDatabase(orderNumber, customerName, deliveryDate, con);
                }
                
                JSONArray dTaskArray = (JSONArray) oObject.get("DepartmentTasks");
                for (Object object1 : dTaskArray) //Iterates over an array of departmentTasks within each production order
                {
                    JSONObject dTaskObject = (JSONObject) object1;
                    
                    JSONObject dObject = (JSONObject) dTaskObject.get("Department");
                    String departmentName = (String) dObject.get("Name");
                    
                    String endDateString = (String) dTaskObject.get("EndDate");
                    Date endDate = formatDateString(endDateString);
                    
                    String startDateString = (String) dTaskObject.get("StartDate");
                    Date startDate = formatDateString(startDateString);
                    
                    boolean finishedTask = (boolean) dTaskObject.get("FinishedOrder");
                    
                    if (id > 0 && !checkIfTaskExists(con, departmentName, id))
                    {
                        uploadDTaskToDatabase(departmentName, startDate, endDate, finishedTask, 0, con, id);
                    }
                    
                }
                
            }
        }
    }
    
    public int uploadPOrderToDatabase(String orderNumber, String customerName, Date deliveryDate, Connection con) throws SQLException
    {
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO ProductionOrder VALUES (?,?,?)", Statement.RETURN_GENERATED_KEYS);
        
        java.sql.Date sqlDate = new java.sql.Date(deliveryDate.getTime());
        pstmt.setString(1, orderNumber);
        pstmt.setString(2, customerName);
        pstmt.setDate(3, sqlDate);
        
        pstmt.execute();
        
        ResultSet rs = pstmt.getGeneratedKeys();
        int id = 0;
        if (rs.next())
        {
            id = rs.getInt(1);
        }
        return id;
    }
    
    public void uploadDTaskToDatabase(String departmentName, Date startDate, Date endDate, boolean finishedTask, int timeOffset, Connection con, int id) throws SQLException
    {
        
        PreparedStatement pstmt1 = con.prepareStatement("INSERT INTO DepartmentTask VALUES (?,?,?,?,?,?)");
        
        java.sql.Date sqlStartDate = new java.sql.Date(startDate.getTime());
        java.sql.Date sqlEndDate = new java.sql.Date(endDate.getTime());
        
        pstmt1.setInt(1, id);
        pstmt1.setString(2, departmentName);
        pstmt1.setDate(3, sqlStartDate);
        pstmt1.setDate(4, sqlEndDate);
        pstmt1.setInt(5, timeOffset);
        pstmt1.setBoolean(6, finishedTask);
        
        pstmt1.execute();
    }
    
    private Date formatDateString(String dateString)
    {
        Long milli = Long.parseLong(dateString.substring(6, dateString.indexOf("+")));
        Date newDate = new Date(milli);
        return newDate;
    }
    
    public boolean checkIfTaskExists(Connection con, String departmentName, int id) throws SQLException
    {
        boolean existingTask = false;
        PreparedStatement pstmt1 = con.prepareStatement("SELECT ProdId, DepartmentName FROM DepartmentTask "
                + "WHERE DepartmentName = (?) AND ProdId = (?)");
        pstmt1.setString(1, departmentName);
        pstmt1.setInt(2, id);
        
        ResultSet rs = pstmt1.executeQuery();
        if (rs.next())
        {
            existingTask = true;
        }
        return existingTask;
    }
    
    public boolean checkIfOrderExists(Connection con, String orderNumber) throws SQLException
    {
        boolean existingOrder = false;
        PreparedStatement pstmt = con.prepareStatement("SELECT OrderNr FROM ProductionOrder WHERE OrderNr = (?)");
        pstmt.setString(1, orderNumber);
        
        ResultSet rs = pstmt.executeQuery();
        
        if (rs.next())
        {
            existingOrder = true;
        }
        return existingOrder;
    }
    
    public void checkJSONFolder(File folder) throws IOException, FileNotFoundException, ParseException, SQLException
    {
        for (File listFile : folder.listFiles())
        {
            String path = listFile.getPath();
            uploadJSON(path);
        }
    }
}
