package com.mbit.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class asSetVariableSentence extends UDF {

	public Text evaluate(String variableName, Text variableValue) {
		if (variableValue == null || variableName==null)
			return null;
		return new Text("set "+variableName.toString()+"="+ variableValue.toString()+";");
	}
	
	public Text evaluate(String variableName, int variableValue) {
		if (variableName==null)
			return null;
		return new Text("set "+variableName.toString()+"="+ variableValue+";");
	}
	
	public Text evaluate(Text variableName, Text variableValue) {
		if (variableValue == null || variableName==null)
			return null;
		return new Text("set "+variableName.toString()+"="+ variableValue.toString()+";");
	}
	public Text evaluate(String variableName, String variableValue) {
		if (variableValue == null || variableName==null)
			return null;
		return new Text("set "+variableName+"="+ variableValue+";");
	}
}
