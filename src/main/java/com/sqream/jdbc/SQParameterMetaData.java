package com.sqream.jdbc;

import java.sql.ParameterMetaData;
import java.sql.Types;
import java.sql.SQLException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.sqream.jdbc.Connector.ConnException;


public class SQParameterMetaData implements ParameterMetaData{
	
	  Connector conn;
	  ColumnMetadata[] meta;
	  int param_count = 0;
	  
	  String param_name = "";
	  String param_type = "";
	  boolean is_param_nullable = false;
	  int param_scale = 0;
	  int param_percision = 0;
	  
	  Map<String, Integer> sqream_to_sql = Stream.of(new Object[][] { 
		    { "ftBool", Types.BOOLEAN }, 
		    { "ftUByte", Types.TINYINT }, 
		    { "ftShort", Types.SMALLINT }, 
		    { "ftInt", Types.INTEGER }, 
		    { "ftLong", Types.BIGINT }, 
		    { "ftFloat", Types.REAL }, 
		    { "ftDouble", Types.DOUBLE }, 
		    { "ftDate", Types.DATE }, 
		    { "ftDateTime", Types.TIMESTAMP }, 
		    { "ftVarchar", Types.VARCHAR }, 
		    { "ftBlob", Types.NVARCHAR }, 
		}).collect(Collectors.toMap(data -> (String) data[0], data -> (Integer) data[1]));
	  
	  
	  
	  public SQParameterMetaData(Connector _conn) throws SQLException {
		  
	     if (_conn == null)
	    	 throw new SQLException("null connector object passed to SQParameterMetaData");
	     
	     conn = _conn;
	     // A query is marked "INSERT" in Connector.java when we get a non-empty queryTypeIn - network insert
    	 param_count = (conn.get_query_type().equals("INSERT")) ? conn.get_row_length() : 0;
    	 
	  }
	  
	  
	  boolean _validate_index (int param_index) throws SQLException {
		  
		  if (param_index > param_count || param_index < 1)
			  throw new SQLException ("parameter index " + param_index + " out of range. Number of parameters for this query is: " + param_count);
		  
		  return true;
	  }
	  
	  
	  @Override
	  public int getParameterCount() {
		  
	     return param_count;
	  }

	  
	  @Override
	  public String getParameterClassName(int param_index) throws SQLException {
	    
		_validate_index(param_index);
	    
	    try {
	    	param_name =  conn.get_col_name(param_index);
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to get parameter name in SQParameterMetaData:\n" + e);
		}
	    
	    return param_name;
	  }
	  
	  
	  @Override
	  public int getParameterType(int param_index) throws SQLException {
	   
		_validate_index(param_index);
		
		try {
			param_type = conn.get_col_type(param_index);
		} catch (ConnException e) {
			e.printStackTrace();
		}
	    
	    return sqream_to_sql.get(param_type);
	  }
	  
	  
	  // ParameterModes explained here - 
	  // https://docs.oracle.com/javase/tutorial/jdbc/basics/storedprocedures.html
	  // Looks like we're always IN
	  @Override
	  public int getParameterMode(int param_index) throws SQLException {
	    
		  _validate_index(param_index);
	    
		  return ParameterMetaData.parameterModeIn;
	  }
	  
	  
	  @Override
	  public int getPrecision(int param_index) throws SQLException {
	    
		  _validate_index(param_index);
		  
		  try {
				return conn.get_col_size(param_index);
			} catch (ConnException e) {
				e.printStackTrace();
				throw new SQLException ("Connector exception when trying to check parameter precision in SQParameterMetaData:\n" + e);
			}
	  }
	  
	  
	  @Override
	  public int getScale(int param_index) throws SQLException {
	    
		_validate_index(param_index);
		  
	    try {
			param_type = conn.get_col_type(param_index);
		} catch (ConnException e) {
			e.printStackTrace();
		}
	    
	    if (param_type.equals("ftFloat"))
	    	param_scale = 4;
    	else if (param_type.equals("ftDouble"))
    		param_scale = 8;
    	else 
    		param_scale = 0;
	    
	    return param_scale;
	  }
	  
	  
	  @Override
	  public int isNullable(int param_index) throws SQLException {
	    
		  _validate_index(param_index);
	    
	    try {
	    	is_param_nullable =  conn.is_col_nullable(param_index);
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to get parameter nullability in SQParameterMetaData:\n" + e);
		}
	    
	    return is_param_nullable ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNullableUnknown;
	  }
	  
	  
	  @Override
	  public String getParameterTypeName(int param_index) throws SQLException {
		  
		  _validate_index(param_index);
		  
		  try {
			  param_type =  conn.get_col_type(param_index);
			} catch (ConnException e) {
				e.printStackTrace();
				throw new SQLException ("Connector exception when trying to get parameter type name in SQParameterMetaData:\n" + e);
			}
		    
		    return param_type;
	  }
	  
	  
	  @Override
	  public boolean isSigned(int param_index) throws SQLException {
	    
		  _validate_index(param_index);  
	    
        try {
			return Arrays.asList("ftShort", "ftInt", "ftLong", "ftFloat", "ftDouble").contains(conn.get_col_type(param_index));
		} catch (ConnException e) {
			e.printStackTrace();
			throw new SQLException ("Connector exception when trying to check if parameter is signed in SQParameterMetaData:\n" + e);
		}
	  
	  }

	  
	  // Methods inherited from interface java.sql.Wrapper
	  @Override
	  public boolean isWrapperFor(Class<?> iface) throws SQLException {
	    
		  return iface.isAssignableFrom(getClass());
	  }
	  
	  
	  // Methods inherited from interface java.sql.Wrapper
	  @Override
	  public <T> T unwrap(Class<T> iface) throws SQLException {
	    
		  if (iface.isAssignableFrom(getClass())) {
	      return iface.cast(this);
	    }
	    throw new SQLException("Cannot unwrap to " + iface.getName());
	  }
	  
}
