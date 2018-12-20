package com.sqream.jdbc;

class Tuple<X, Y> {

    /**
     * @param args
     */
     X host; 
     Y port;
    
    Tuple(X x, Y y) {
        this.host = x;
        this.port = y; 
             
  } 
    void Add(Tuple<X, Y> v)
    {
  
    	this.host=v.host;
    	this.port=v.port;
    	
    }
    static void main(String[] args) {
        // TODO Auto-generated method stub

    }

}
