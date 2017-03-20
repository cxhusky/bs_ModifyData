package cn.cslg.modifydata.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import cn.cslg.modifydata.bean.ModifyArtistMapperOutKey;

/**
 * 对照两个文件的内容，修正badid为goodid
 * 
 * @author xwtech
 */
public class ModifyArtistReducer extends
		Reducer<ModifyArtistMapperOutKey, Text, Text, NullWritable> {
	private NullWritable outV = NullWritable.get();
	private Text outK = new Text();
	@Override
	protected void reduce(
			ModifyArtistMapperOutKey k,
			Iterable<Text> values,
			Reducer<ModifyArtistMapperOutKey, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();
		Text first = iterator.next();
		String[] splits = first.toString().trim().split(" ");
		if (splits != null) {
			if (1 == splits.length) {		
				//第一个是artist_alias文件中的,后面的修改key值输出
				String goodid = splits[0].trim();
				while (iterator.hasNext()) {
					outK.set(new Text(goodid + " " + 
							((Text) iterator.next()).toString()));
				}
				context.write(outK, outV);
			} else if (2 == splits.length) { 
				//第一个是User_artist_data文件中的直接输出，后面的也直接输出.
				context.write(new Text(k.getId().toString() + " " + 
							first.toString()), outV);
				while (iterator.hasNext()) {
					outK.set(new Text(k.getId().toString() + " " + 
							((Text) iterator.next()).toString()));
					context.write(outK, outV);
				}
			} 
		}
	}
}
