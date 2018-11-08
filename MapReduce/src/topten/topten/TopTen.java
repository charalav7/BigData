package topten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TopTen {
    // This helper function parses the stackoverflow into a Map for us.
    public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
			try {
				String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
	    		for (int i = 0; i < tokens.length - 1; i += 2) {
					String key = tokens[i].trim();
					String val = tokens[i + 1];
					map.put(key.substring(0, key.length() - 1), val);
	    		}
			} catch (StringIndexOutOfBoundsException e) {
	    		System.err.println(xml);
			}

		return map;
    }

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
        TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Define the input value as a string
            String inputValue = value.toString();

            // Declare the pattern to look for in the input file string
            Pattern pattern = Pattern.compile("<row(.+?)/>");
            Matcher matcher = pattern.matcher(inputValue);

            // While the match exists we get the group that corresponds to each User
            // We also add the corresponding tag to the string to be in compliance with the transformXmlToMap method
            if (matcher.find()) {
                // Get the matching group
                String singleUser = "<row" + matcher.group(1) + "/>";

                // Call the transformXmlToMap method to get a hashmap for this specific user record string
                Map<String, String> map = transformXmlToMap(singleUser);

                // Avoid Id key to be equal to null or empty value
                // Get the reputation integer for this specific User
                // Put the <reputation, userRecord> key-value pair in the TreeMap
                if (!map.get("Id").equals("null") && !map.get("Id").equals("")) {
                    int reputation = Integer.valueOf(map.get("Reputation"));
                    Text userRecord = new Text(singleUser);
                    repToRecordMap.put(reputation, userRecord);
                }
            }

            // Here we check the size of our TreeMap to not exceed 10 entries.
            // We omit the keys/reputations with lower value
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Output our ten records to the reducers with a null key
            // Now we simply iterate through topTenRepToRecordMap values to provide to the reducer our entries
            for (Text record : repToRecordMap.values()) {
                context.write(NullWritable.get(), record);
            }
		}
    }

    public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
        // we set the constructor to store keys in descending order to have the best reputation record at the top
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Comparator.reverseOrder());

		// Declare a TreeMap for Reputation to Id record with the best reputation record to be at the top
        private TreeMap<Integer, Integer> repToIdMap = new TreeMap<>(Comparator.reverseOrder());

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // The Reducer will have one input and that will be -->
            // (nullWritable, [TextRecord1, TextRecord2, ... , TextRecord10])
            // Iterate through all given Text records and store them in a TreeMap
            for (Text val : values) {
                // Call the transformXmlToMap method to get a hashmap for this specific User record
                Map<String, String> map = transformXmlToMap(val.toString());
                // Get the reputation integer for this specific User
                // Put the <reputation, userRecord> key-value pair in the TreeMap
                int reputation = Integer.valueOf(map.get("Reputation"));
                Text userRecord = new Text(val);
                repToRecordMap.put(reputation, userRecord);
                // Get the id of this specific record
                int id = Integer.valueOf(map.get("Id"));
                repToIdMap.put(reputation, id);
            }

            // Iterate through all repToIdMap to store values of Id and Reputation to the topten table
            for (Map.Entry<Integer, Integer> entry : repToIdMap.entrySet()) {
                int rep = entry.getKey();
                int id = entry.getValue();

                System.out.println("ID ---> " + id + ", " + "REPUTATION ---> " + rep);

                // create hbase put with rowkey as the id of the record
                Put insHBase = new Put(Bytes.toBytes(id));

                // insert id  and rep values to hbase
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));
                insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));

                // write data to Hbase table
                context.write(null, insHBase);
            }

		}
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(TopTen.class);

        // One input for reducer that contains all potential top ten records
        job.setNumReduceTasks(1);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        // define output table
        TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
        job.waitForCompletion(true);

    }
}
