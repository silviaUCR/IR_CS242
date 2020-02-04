package edu.ucr.ir.data;

import java.io.File;
import java.io.IOException;

import  com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

public class JsonUtils {
    public static void writeJsonToFile(String filename, Object obj) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        om.writeValue(new File(filename), obj);
    }

    public static Object readJsonFromFile(String filename, Class<?> objType) throws IOException {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        File file = new File(filename);
        return om.readValue(file, objType);
    }

    

}
