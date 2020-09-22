/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cqx.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

    public static void main(String[] args) throws Exception {
        //检查参数
        if (args.length != 2) {
            System.out.println("Args need inputFile and outputFile , please check!");
            System.exit(-1);
        }
        final String inputFile = args[0];
        final String outFile = args[1];
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/batch/index.html
         *
         * and the examples
         *
         * https://flink.apache.org/docs/latest/apis/batch/examples.html
         *
         */

        //使用 ExecutionEnvironment 创建 DataSet
        DataSet<String> lines = env.readTextFile(inputFile);

        //Transformations 操作
        //切分压平
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按空格分隔；将每个单词与 1 组合，形成一个元组；放入到 Collector 集合，并输出
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //聚合
        //离线计算实现的是分组聚合，调用的是groupBy()。实时计算调用的是 keyBy()
        AggregateOperator<Tuple2<String, Integer>> sumed = wordAndOne.groupBy(0).sum(1);

        //将结果保存成文本(或者保存到 hdfs 等)
        //setParallelism(1)：设置并行度
        sumed.writeAsText(outFile).setParallelism(1);

        // execute program
        env.execute("BatchWordCount");
    }
}
