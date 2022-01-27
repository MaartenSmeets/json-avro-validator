import com.demo.avro.Customer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.PropertyBindingException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
//import org.apache.avro.util.RandomData;

import java.io.IOException;
import java.io.InputStream;

public class AvroTest {
    public static void main(String[] args) throws IOException {
        AvroTest me = new AvroTest();
        ClassLoader classLoader = me.getClass().getClassLoader();
        InputStream is_schema = classLoader.getResourceAsStream("file.avsc");
        InputStream is_json = classLoader.getResourceAsStream("file.json");
        Schema schema = new Schema.Parser().parse(is_schema);
        //Generate a random JSON which conforms to the AVRO schema
        //RandomData rd = new RandomData(schema, 1);
        //System.out.println(rd.iterator().next());

        System.out.println("Checking against schema directly using Apache AVRO library");
        try {
            DatumReader reader = new GenericDatumReader(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, is_json);
            reader.read(null, decoder);
            System.out.println("No errors");
        } catch (AvroTypeException e) {
            System.out.println(e.getMessage());
        }

        is_json = classLoader.getResourceAsStream("file.json");
        System.out.println("Checking against Java classes using Apache AVRO library");
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, is_json);
        SpecificDatumReader<Customer> reader = new SpecificDatumReader<>(Customer.class);
        try {
            Customer au = reader.read(null, decoder);
            System.out.println("No errors");
        } catch (AvroTypeException e) {
            System.out.println(e.getMessage());
        }

        System.out.println("Checking against Java classes using Jackson ObjectMapper");
        is_json = classLoader.getResourceAsStream("file.json");
        ObjectMapper mapper = new ObjectMapper();
        try {
            // DeSerializing the JSON to Avro class, but this doesn't check for Schema restrictions
            Customer obj = mapper.readValue(is_json, Customer.class);
            // Encoding the class and serializing to raw format, this step validates based on schema
            obj.toByteBuffer();
            System.out.println("No errors");
        } catch (PropertyBindingException e) {
            System.out.println(e.getMessage());
        }
    }
}