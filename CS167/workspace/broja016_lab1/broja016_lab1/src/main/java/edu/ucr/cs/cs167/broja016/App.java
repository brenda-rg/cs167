package edu.ucr.cs.cs167.broja016;

import java.util.function.Function;

/**
 * Hello world!
 *
 */


public class App 
{

    public static void main( String[] args )
    {

        //System.out.println( "Hello World!" );
        callFunc(args);

        /*System.out.println( "Calling IsEven" );
        IsEven isEven = new IsEven();
        System.out.println(isEven.apply(5));*/

    }
    /* Even number function below*//*
    public static void printEvenNumbers(int from, int to) {
        System.out.printf("Printing even numbers in the range [%d,%d]\n", from, to);
        for(int i =from; i <= to; i++) {
            if(i%2 == 0) System.out.println(i);
        }
    }
    *//* Div by 3 function below*//*
    public static void printNumbersDivisibleByThree(int from, int to) {
        System.out.printf("Printing numbers divisible by 3 in the range [%d,%d]\n", from, to);
        for(int i =from; i <= to; i++) {
            if(i%3 == 0) System.out.println(i);
        }
    }*/
    /* Call function below*/
    public static void callFunc(String[] args) {
        //check for 3 param
        if (args.length < 3) {
            System.out.println("Error: At least three parameters expected, from, to, and base.");
            System.exit(1); //incorrect # of arguments
        }

        //define params
        int from = Integer.parseInt(args[0]);
        int to = Integer.parseInt(args[1]);
        String s = args[2];
        String regex;
        boolean combine = false;

        if(s.indexOf(',') != -1) {
            regex = "[,]";
            combine = true;
        }
        else {
            regex = "[v]";
        }

        String[] temp = s.split(regex);

        int[] bases = new int[temp.length];

        for(int i = 0; i < temp.length; ++i) {
            bases[i] = Integer.parseInt(temp[i]);
        }


        Function<Integer, Boolean>[] filters = new Function[bases.length];
        //loop and create filters
        for(int i = 0; i < filters.length; ++i) {
            int curr = i;
            filters[i] = x -> x% bases[curr] == 0;
        }
        //delimiter is ',', use and
        if(combine) {
            Function<Integer, Boolean> filter = combineWithAnd(filters);
            printNumbers(from, to, filter);
        }
        //delimiter is 'v', use or
        else {
            Function<Integer, Boolean> filter = combineWithOr(filters);
            printNumbers(from, to, filter);
        }



        //initialize 3rd param function
        IsEven isEven = new IsEven();
        IsDivisibleByThree divByThree = new IsDivisibleByThree();
        Function<Integer, Boolean> divisibleByTen = x -> x % 10 == 0;
        Function<Integer, Boolean> divisibleByFive = new Function<Integer, Boolean>() {
            @Override
            public Boolean apply(Integer x) {
                return x % 5 == 0;
            }
        };

        //Function<Integer, Boolean> divisibleByBase = x -> x% base == 0;

        //check base
        //printNumbers(from, to, divisibleByBase);

        //added base 5 and 10 functions & changed to printNumbers
        /*if(base == 2) {
            printNumbers(from, to, isEven);
        }
        else if(base == 3) {
            printNumbers(from, to, divByThree);
        }
        else if(base == 5){
            printNumbers(from,to,divisibleByFive);
        }
        else if(base == 10) {
            printNumbers(from,to, divisibleByTen);
        }else { //error if base not in functions
            System.out.println("Error: Base should have a value of either 2, 3, 5, or 10.");
            System.exit(2); //invalid base value
        }*/

        //added base 2 and 3 functions
        /*if (base == 2) {
            printEvenNumbers(from, to);

        } else if (base == 3) {
            printNumbersDivisibleByThree(from, to);
        } else {
            System.out.println("Error: Base should have a value of either 2 or 3.");
            System.exit(2); //invalid base value
        }*/

    }

    static class IsEven implements Function<Integer, Boolean> {
        @Override
        public Boolean apply(Integer x) {
            return x % 2 == 0;
        }
    }

    static class IsDivisibleByThree implements Function<Integer, Boolean> {
        @Override
        public Boolean apply(Integer x) {
            return x % 3 == 0;
        }
    }

    public static void printNumbers(int from, int to, Function<Integer, Boolean> filter) {
        System.out.printf("Printing numbers in the range [%d,%d]\n", from, to);
        for(int i = from; i <= to; ++i) {
            if (filter.apply(i)) {
                System.out.println(i);
            }
        }
    }

    public static Function<Integer, Boolean> combineWithAnd(Function<Integer, Boolean> ... filters) {
        return x -> {
            //Go through all filters
            // If even one filter doesn't apply for curr int then don't print
            for (Function<Integer, Boolean> filter : filters) {
                if (!filter.apply(x)) {
                    return false;
                }
            }
            //All filters apply so you can print
            return true;
        };
    }

    public static Function<Integer, Boolean> combineWithOr(Function<Integer, Boolean> ... filters) {
        return x-> {
            //Go through all filters
            // If even one filter does apply for curr int then print
            for (Function<Integer, Boolean> filter : filters) {
                if (filter.apply(x)) {
                    return true;
                }
            }
            //No filters apply so don't print
            return false;
        };
    }


}







