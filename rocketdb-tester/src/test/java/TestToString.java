import org.junit.Test;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class TestToString {
    @Test
    public void test1() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10 * 10000; i++) {
            String.valueOf(i).getBytes();
        }
        System.out.println(System.currentTimeMillis() - start);
    }



    @Test
    public void test3() {

        System.out.println((int)'\n');
    }

    @Test
    public void test4() {
        String das = "饕餮";
        System.out.println(das.length());
    }

    @Test
    public void test5() {
        Object[] objects = new Object[1000 * 1000 * 50];
        for (int i = 0; i < objects.length; i++) {
            objects[i] = new Object();
        }
        System.out.println(objects.length);
    }

    @Test
    public void test6() {
        int[][] nums = new int[1000 * 1000 * 50][];
        for (int i = 0; i < nums.length; i++) {
            nums[i] = new int[0];
        }
        System.out.println(nums.length);
    }
}
