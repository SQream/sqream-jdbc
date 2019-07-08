package com.sqream.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class SQParameterMetaData implements ParameterMetaData{
	
	  private final SQConnection _connection;
	  private final int[] _oids;

	  public SQParameterMetaData(SQConnection connection, int[] oids) {
	    _connection = connection;
	    _oids = oids;
	  }

	  public String getParameterClassName(int param) throws SQLException {
	    checkParamIndex(param);
	    
	    return "poo";
	    //return _connection.getTypeInfo().getJavaClass(_oids[param - 1]);
	  }

	  public int getParameterCount() {
	    return _oids.length;
	  }

	  
	  // CallableStatements may have one output, but ignore that for now.
	  public int getParameterMode(int param) throws SQLException {
	    checkParamIndex(param);
	    return ParameterMetaData.parameterModeIn;
	  }

	  public int getParameterType(int param) throws SQLException {
	    checkParamIndex(param);
	    return 0; //_connection.getTypeInfo().getSQLType(_oids[param - 1]);
	  }

	  public String getParameterTypeName(int param) throws SQLException {
	    checkParamIndex(param);
	    return "poo2"; //_connection.getTypeInfo().getPGType(_oids[param - 1]);
	  }

	  public int getPrecision(int param) throws SQLException {
	    checkParamIndex(param);
	    return 0;
	  }

	  public int getScale(int param) throws SQLException {
	    checkParamIndex(param);
	    return 0;
	  }

	  // we can't tell anything about nullability
	  public int isNullable(int param) throws SQLException {
	    checkParamIndex(param);
	    return ParameterMetaData.parameterNullableUnknown;
	  }

	  public boolean isSigned(int param) throws SQLException {
	    checkParamIndex(param);
	    return true; //_connection.getTypeInfo().isSigned(_oids[param - 1]);
	  }

	  private void checkParamIndex(int param) throws SQLException {
	    if (param < 1 || param > _oids.length) 
	      throw new SQLException("The parameter index is out of range: {0}, number of parameters: {1}.");
	  }

	  public boolean isWrapperFor(Class<?> iface) throws SQLException {
	    return iface.isAssignableFrom(getClass());
	  }

	  public <T> T unwrap(Class<T> iface) throws SQLException {
	    if (iface.isAssignableFrom(getClass())) {
	      return iface.cast(this);
	    }
	    throw new SQLException("Cannot unwrap to " + iface.getName());
	  }
}
