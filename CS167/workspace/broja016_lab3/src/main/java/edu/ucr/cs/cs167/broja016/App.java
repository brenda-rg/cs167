package edu.ucr.cs.cs167.broja016;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws IOException
    {
        //if incorrect # args
        if(args.length != 3) {
            System.err.println("Incorrect number of arguments! Expected three arguments.");
            System.exit(-1);
        }

        String input = args[0];
        long offset = Long.parseLong(args[1]);
        long length = Long.parseLong(args[2]);

        Path inputPath = new Path(input);
        Configuration config = new Configuration();

        try {
            FileSystem inputFS = inputPath.getFileSystem(config);

            if (!inputFS.exists(inputPath)) {
                System.err.printf("Input file '%s' does not exist!\n", inputPath);
                System.exit(-1);
            }
        } catch (IOException e){
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }

        FileSystem inputFS = inputPath.getFileSystem(config);

        if (!inputFS.exists(inputPath)) {
            System.err.printf("Input file '%s' does not exist!\n", inputPath);
            System.exit(-1);
        }
        FSDataInputStream inputStream = inputFS.open(inputPath);

        inputStream.seek(offset);
        if(offset != 0) {
            //read until next newline
            readLine(inputStream);
        }

        int numMatchingLines = 0;

        while(inputStream.getPos() <= (offset + length)){
            String line = readLine(inputStream);
            if (line.contains("200")) {
                numMatchingLines++;
            }
            System.out.println(line.trim());
        }

        System.out.println("Split length: "+ length);
        System.out.println("Actual bytes read: "+ (inputStream.getPos() - offset));
        System.out.println("Number of matching lines: " + numMatchingLines);


    }

    //helper function provided
    //reads line
    public static String readLine(FSDataInputStream input) throws IOException {
        StringBuffer line = new StringBuffer();
        int value;
        do {
            value = input.read();
            if (value != -1)
                line.append((char)value);
        } while (value != -1 && value != 13 && value != 10);
        return line.toString();
    }
}

