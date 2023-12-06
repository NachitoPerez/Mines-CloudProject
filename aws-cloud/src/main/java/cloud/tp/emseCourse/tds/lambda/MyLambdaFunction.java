package fr.emse.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class MyLambdaFunction implements RequestHandler<String, String> {

	public String handleRequest(String arg, Context context) {
		return arg;
	}
}
