import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Created by lijs
 * on 2017/8/3.
 */
public class TestArrayOutputStream {
    @Test
    public void test1() {
        ByteArrayOutputStream stream = new ByteArrayOutputStream(10280);
        stream.write(1);
        stream.write(2);
        stream.write(3);
        stream.reset();
        System.out.println(Arrays.toString(stream.toByteArray()));
    }

    @Test
    public void test2() {

    }
}
