package cn.cslg.modifydata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cn.cslg.modifydata.bean.ModifyArtistMapperOutKey;
import cn.cslg.modifydata.mapreduce.ArtistAliasMapper;
import cn.cslg.modifydata.mapreduce.ModifyArtistReducer;
import cn.cslg.modifydata.mapreduce.UserArtistDataMapper;

/**
 * 修正artist id job
 * @author xwtech
 */
public class ModifyArtistIdJob extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		Path firstinput = new Path(conf.get("fin"));
		Path secondinput = new Path(conf.get("sin"));
		Path output = new Path(conf.get("out"));

		Job modifyArtistJob = Job.getInstance(conf);
		modifyArtistJob.setJobName("modify artist bad id job");
		modifyArtistJob.setJarByClass(ModifyArtistIdJob.class);

		modifyArtistJob.setMapOutputKeyClass(ModifyArtistMapperOutKey.class);
		modifyArtistJob.setMapOutputValueClass(Text.class);

		modifyArtistJob.setReducerClass(ModifyArtistReducer.class);
		modifyArtistJob.setOutputKeyClass(Text.class);
		modifyArtistJob.setOutputValueClass(NullWritable.class);

		MultipleInputs.addInputPath(modifyArtistJob, firstinput,
				TextInputFormat.class, ArtistAliasMapper.class);
		MultipleInputs.addInputPath(modifyArtistJob, secondinput,
				TextInputFormat.class, UserArtistDataMapper.class);
		modifyArtistJob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(modifyArtistJob, output);

		// 压缩
		FileOutputFormat.setCompressOutput(modifyArtistJob, true);
		FileOutputFormat.setOutputCompressorClass(modifyArtistJob,
				BZip2Codec.class);
		// modifyArtistJob.setNumReduceTasks(6);
		return modifyArtistJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ModifyArtistIdJob(), args));
	}

}
