package be.ugent.idlab.knows.mappingweaver.fno;

public final class NumericArrayFunctions {

    private NumericArrayFunctions() {
    }

    public static Integer[] splitNumber(Double number) {
        int whole = number.intValue();
        int fraction = (int) Math.round((number - whole) * 100);
        return new Integer[]{whole, fraction};
    }
}
