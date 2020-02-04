package edu.ucr.ir.data;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class JSONArray {
    public JSONArray parseJSONFile(){
        //Get the JSON file, in this case is in ~/resources/test.json
        InputStream jsonFile =  getClass().getResourceAsStream("");
        Reader readerJson = new InputStreamReader(jsonFile);
        //Parse the json file using simple-json library
        Object fileObjects= JSONValue.parse(readerJson);
        JSONArray arrayObjects=(JSONArray)fileObjects;
        return arrayObjects;
    }
}
