import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;


import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {

    public static void main(String[] args) {

       String sql="CALL spark_catalog.system.rewrite_data_files(\n" +
               "\ttable=>'dl_test.ods_cowell_order_order_base_stream_v2',\n" +
               "\toptions=>map(\n" +
               "\t\t'max-concurrent-file-group-rewrites','200',\n" +
               "\t\t'target-file-size-bytes','134217728',\n" +
               "\t\t'min-input-files','5'\n" +
               "\t),\n" +
               "\twhere=>'dt>=\"%2%\"'\n" +
               ");";



        String input = "The quick brown fox jumps over the lazy dog.";
        String regex = "%\\d%"; // 正则表达式，匹配单词"the"





        if(sql.contains("where=>'dt")){
            Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(sql);
            int daysAgo = 0; // 你想要生成的天数
            String group = "";
            while (matcher.find()) {
                group = matcher.group();
                String group2 = group.replace("%", "");
                System.out.println("Found the substring: " + group2);
                daysAgo = Integer.valueOf(group2).intValue();
            }
            LocalDate date = LocalDate.now().minusDays(daysAgo);
            String dataStr = date.toString().replace("-", "");
            System.out.println("指定天数的日期是: " + dataStr);
            sql = sql.replace(group, "%s");
            sql = String.format(sql, dataStr);
            System.out.println("sql= " + sql);
        }



//
//        int numEntries = 1000000;
//        double errorRate = 0.01;
//        BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), numEntries, errorRate);
//
//        // 将企业id列中的所有值添加到 Bloom Filter 中
////        List<Integer> enterpriseIds = getEnterpriseIdsFromData(); // 从数据中获取企业id列的所有值
//        List<Integer> enterpriseIds = new ArrayList<>(); // 从数据中获取企业id列的所有值
//        enterpriseIds.add(1);
//        enterpriseIds.add(2);
//        enterpriseIds.add(3);
//        enterpriseIds.add(4);
//        enterpriseIds.add(5);
//        enterpriseIds.add(6);
//        enterpriseIds.add(7);
//        enterpriseIds.add(8);
//        for (Integer enterpriseId : enterpriseIds) {
//            bloomFilter.put(enterpriseId);
//        }
//
//        // 估算哈希值分布情况
//        int numBuckets = 3; // 设置初始的分区数量
//        long[] counts = new long[numBuckets];
//        for (int i = 0; i < numEntries; i++) {
////            int hashValue = Math.abs(bloomFilter.hash(i).asInt() % numBuckets);
//            int hashValue = Math.abs(bloomFilter.hashCode() % numBuckets);
//            counts[hashValue]++;
//
////            bloomFilter.hashCode()
//        }
//
//        // 根据哈希值分布情况选择最终的分区数量
//        int finalNumBuckets = chooseNumBuckets(counts);
//        System.out.println("Final number of buckets: " + finalNumBuckets);
    }


    private static int chooseNumBuckets(long[] counts) {
        int numBuckets = counts.length;
        long total = 0;
        for (long count : counts) {
            total += count;
        }
        double avgCount = (double) total / numBuckets;
        int finalNumBuckets = 0;
        double maxDeviation = Double.MIN_VALUE;
        for (int i = 1; i < numBuckets; i++) {
            long leftTotal = 0;
            for (int j = 0; j < i; j++) {
                leftTotal += counts[j];
            }
            long rightTotal = total - leftTotal;
            double leftRatio = (double) leftTotal / total;
            double rightRatio = (double) rightTotal / total;
            double deviation = Math.abs(leftRatio - rightRatio);
            if (deviation > maxDeviation && leftTotal >= avgCount && rightTotal >= avgCount) {
                maxDeviation = deviation;
                finalNumBuckets = i;
            }
        }
        if (finalNumBuckets == 0) {
            finalNumBuckets = numBuckets;
        }
        return finalNumBuckets;
    }

}
