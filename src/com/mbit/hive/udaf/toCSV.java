package com.mbit.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class toCSV extends UDAF     {

    public static class AverageUDAFEvaluator implements UDAFEvaluator{
    	 
        /**
         * Use item class to serialize intermediate computation
         */
         
        private String cadenaCSV = null;
         
        /**
         * function: Constructor
         */
        public AverageUDAFEvaluator(){
            super();
            init();
        }
         
        /**
         * function: init()
         * Its called before records pertaining to a new group are streamed
         */
        public void init() {
        	cadenaCSV = "";          
        }
         
        /**
         * function: iterate
         * This function is called for every individual record of a group
         * @param value
         * @return
         * @throws HiveException 
         */
        public boolean iterate(String field) throws HiveException{
            if(cadenaCSV == null)
                throw new HiveException("Item is not initialized");
            cadenaCSV += field+";";
            return true;
        }
         
        /**
         * function: terminate
         * this function is called after the last record of the group has been streamed
         * @return
         */
        public String terminate(){            
            return cadenaCSV;
        }
         
        /**
         * function: terminatePartial
         * this function is called on the mapper side and 
         * returns partially aggregated results. 
         * @return
         */
        public String terminatePartial(){           
            return cadenaCSV;
        }
         
         
        /**
         * function: merge
         * This function is called two merge two partially aggregated results
         * @param another
         * @return
         */
        public boolean merge(String another){        
            if(another == null) return true;
            cadenaCSV += another;
            return true;
        }
    }
   
}
