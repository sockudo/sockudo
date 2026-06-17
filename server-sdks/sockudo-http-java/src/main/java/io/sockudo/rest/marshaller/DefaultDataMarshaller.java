package io.sockudo.rest.marshaller;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DefaultDataMarshaller implements DataMarshaller {

   private final Gson gson;

   public DefaultDataMarshaller() {
       gson = new GsonBuilder()
           .disableHtmlEscaping()
           .create();
   }

   public DefaultDataMarshaller(Gson customGsonInstance) {
      gson = customGsonInstance;
   }

   public String marshal(final Object data) {
      return gson.toJson(data);
   }
}
