package be.ugent.idlab.knows.values;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Value;
import org.jspecify.annotations.Nullable;

import java.io.*;

/**
 * A wrapper to enable serializability of objects from AMO
 *
 * @param <T>
 */
public abstract class AMLValue<@Nullable T> implements Value {

    protected T value;

    public AMLValue(T value) {
        this.value = value;
    }

    public AMLValue() {
        this.value = null;
    }

    @Nullable
    public T getValue() {
        return value;
    }

    public void setValue(@Nullable T value) {
        this.value = value;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
        if (this.value != null) {
            outputStream.writeObject(this.value);
        }

        byte[] bytes = byteArrayOutputStream.toByteArray();
        byteArrayOutputStream.close();

        out.writeInt(bytes.length); // size of the array for later reconstruction
        out.write(bytes);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];

        in.readFully(bytes);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            // Code assumes that the value has been serialized by the write method above. As such, it can be assumed that
            //      the cast to T is safe. Reading in any other object is undefined behaviour,
            //      with a ClassCastException as a result
            //noinspection unchecked
            this.value = (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
}
