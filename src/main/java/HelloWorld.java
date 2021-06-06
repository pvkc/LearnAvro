import example.avro.User;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HelloWorld {
    public static void main(String[] args) throws IOException {
        // Create users
        User venkata = User.newBuilder().setLastName("Venkata").setFavoriteColor("Red").setFavoriteNumber(9999).build();
        User ram = User.newBuilder().setLastName("Ram").setFavoriteColor("Blue").setFavoriteNumber(999).build();
        System.out.println(venkata);
        // Write user
        HelloWorld.<User>writeToFileWithSchema("/tmp/users.avro", List.of(venkata, ram), User.class);
        // Read User with schema
        //List<User> users = HelloWorld.<User>readFromFileWithSchema("/tmp/users.avro", User.class);
        //System.out.println(users);

        // Read User without schema
        HelloWorld.readFromFileWithOutSchema("/tmp/users.avro");
    }

    public static <T extends SpecificRecordBase> void writeToFileWithSchema(String fileName, List<T> data, Class<T> clazz) throws IOException {
        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<T>(clazz);
        // TODO: Serializing to JSON is possible, use a different encoder (FileWriter).
        try (DataFileWriter<T> dataFileWriter = new DataFileWriter<T>(userDatumWriter)) {
            // Create a new .avro file
            dataFileWriter.create(data.get(0).getSchema(), new File(fileName));
            for (T record : data) {
                // 1 file can have many records, avoid inefficiencies involved duplicating schema
                dataFileWriter.append(record);
            }
        }
    }

    public static <T extends SpecificRecordBase> List<T> readFromFileWithSchema(String fileName, Class<T> clazz) throws IOException {
        // Deserialize Users from disk
        DatumReader<T> userDatumReader = new SpecificDatumReader<T>(clazz);
        DataFileReader<T> dataFileReader = new DataFileReader<T>(new File(fileName), userDatumReader);
        T user = null;
        List<T> users = new ArrayList<>();

        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            // System.out.println(user);
            users.add(user);
        }

        return users;
    }

    public static void readFromFileWithOutSchema(String fileName) throws IOException {
        File file = new File(fileName);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        Schema schema = dataFileReader.getSchema();
        System.out.println(schema.getFields());

        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }


}
