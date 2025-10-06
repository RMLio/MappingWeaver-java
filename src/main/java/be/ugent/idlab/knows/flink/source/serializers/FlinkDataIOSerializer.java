package be.ugent.idlab.knows.flink.source.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public interface FlinkDataIOSerializer<T extends Serializable> extends SimpleVersionedSerializer<T> {

    public T readObjectInputStream(ObjectInputStream bis) throws Exception;

    @Override
    default T deserialize(int version, byte[] byteArray) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
        try (ObjectInputStream in = new ObjectInputStream(bis)) {
            return readObjectInputStream(in);
        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }

    @Override
    default int getVersion() {
        return 1;
    }

    @Override
    default byte[] serialize(T obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            out.flush();
            return bos.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

}
