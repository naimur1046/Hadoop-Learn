package org.danvk.hadoop;
 
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class WordCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {
 
    private final IntWritable ONE = new IntWritable(1);
    private Text word = new Text();

    public static boolean isValidIPAddress(String ipAddress) {
        // Split the IP address into its components
        String[] components = ipAddress.split("\\.");

        // Check if there are exactly 4 components
        if (components.length != 4) {
            return false;
        }

        // Check each component for a valid integer value in the range [0, 255]
        for (String component : components) {
            try {
                int value = Integer.parseInt(component);
                if (value < 0 || value > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false; // Component is not a valid integer
            }
        }

        return true;
    }

    private static int countHits(String []filePaths, String targetIpAddress) {
        int numberOfHits = 0;

        for (String filePath : filePaths) {


            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if (line.contains(targetIpAddress)) {
                        numberOfHits++;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        return numberOfHits;
    }
     public  static HashMap<String, Integer> UniqueValuesOccurrencesExample(String[] stringArray )
     {
         HashMap<String, Integer> occurrencesMap = new HashMap<>();

         for (String str : stringArray) {
             occurrencesMap.put(str, occurrencesMap.getOrDefault(str, 0) + 1);
         }

         return occurrencesMap;

     }


    private static String findMaxOccurrenceKey(HashMap<String, Integer> map) {
        if (map.isEmpty()) {
            return null;
        }

        String maxKey = null;
        int maxCount = Integer.MIN_VALUE;

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxKey = entry.getKey();
                maxCount = entry.getValue();
            }
        }

        return maxKey;
    }


    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(" ");
        String[] csv2=value.toString().split("POST");




        // Number of Hits Counts
//        word.set("Number of hits");
//        int number_of_hits=countHits(csv,"10.216.113.172");
//        String number_of_hits_sting = Integer.toString(number_of_hits);
//        word.set(number_of_hits_sting);
        // Numbe of posts count

//           word.set("Number of posts");
//           int number_of_posts=countHits(csv2,"10.216.113.172");
//        String number_of_posts_sting = Integer.toString(number_of_posts);
//        word.set(number_of_posts_sting);


         // Find the Height Clicking vlaue
//        HashMap<String, Integer> ip_with_occurance=UniqueValuesOccurrencesExample(csv);
//        String maximum_key=findMaxOccurrenceKey(ip_with_occurance);
//        word.set(maximum_key);




//        ArrayList<String> stringList = new ArrayList<>();
//        for(String str:csv)
//        {
//            if(!isValidIPAddress(str)) continue;
//            stringList.add(str);
//        }

        for (String str : csv) {
            if(!isValidIPAddress(str)) continue;
            word.set(str);
            context.write(word, ONE);
        }
    }
}

