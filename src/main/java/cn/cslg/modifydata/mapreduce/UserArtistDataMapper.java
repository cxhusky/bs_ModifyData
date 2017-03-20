package cn.cslg.modifydata.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cn.cslg.modifydata.bean.ModifyArtistMapperOutKey;
import cn.cslg.modifydata.parser.UserArtistDataParser;

public class UserArtistDataMapper extends
		Mapper<LongWritable, Text, ModifyArtistMapperOutKey, Text> {
	private UserArtistDataParser parser = new UserArtistDataParser();
	/*
	 * 输出的Key和Value的对象没有改变，改变的是对象的值
	 */
	private Text outV = new Text();
	private ModifyArtistMapperOutKey outK = new ModifyArtistMapperOutKey(1);
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, ModifyArtistMapperOutKey, Text>.Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			outK.setId(parser.getArtistid());
			outV.set(parser.getUserid() + " " + parser.getPlaycount());
			context.write(outK, outV);
		}
	}
}
