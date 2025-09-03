package edu.ucr.cs.cs167.broja016;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Hello world!
 *
 */
public class App
{
    public static void main( String[] args ) throws IOException
    {
        if(args.length != 2) {
            System.err.println("Incorrect number of arguments! Expected two arguments.");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Configuration config = new Configuration();

        try {
            FileSystem inputFS = inputPath.getFileSystem(config);

            if (!inputFS.exists(inputPath)) {
                System.err.printf("Input file '%s' does not exist!\n", inputPath);
                System.exit(-1);
            }

            FileSystem outputFS = outputPath.getFileSystem(config);
            if (outputFS.exists(outputPath)) {
                System.err.printf("Input file '%s' already exists!\n", outputPath);
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

        FileSystem outputFS = outputPath.getFileSystem(config);
        if (outputFS.exists(outputPath)) {
            System.err.printf("Output file '%s' already exists!\n", outputPath);
            System.exit(-1);
        }


        InputStream inputStream = inputFS.open(inputPath);
        OutputStream outputStream = outputFS.create(outputPath, true);

        long startTime = System.nanoTime();

        byte[] buffer = new byte[4096];
        long bytesCopied = 0;
        int bytesRead;
        String input = inputPath.toString();
        String output = outputPath.toString();

        while ((bytesRead = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, bytesRead);
            bytesCopied += bytesRead;
        }
        long endTime = System.nanoTime();

        System.out.printf("Copied %d bytes from '%s' to '%s' in %f seconds\n",
                bytesCopied, input, output, (endTime - startTime) * 1E-9);
        //check
        System.out.println("Input FileSystem: " + inputFS.getUri());
        System.out.println("Output FileSystem: " + outputFS.getUri());
        System.out.println("Input Path: " + inputPath);
        System.out.println("Output Path: " + outputPath);
    }
}
