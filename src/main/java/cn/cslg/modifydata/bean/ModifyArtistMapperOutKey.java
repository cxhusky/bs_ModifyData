package cn.cslg.modifydata.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 修正artist数据Mapper的输出Key值类型
 * 
 * @author husky
 */
public class ModifyArtistMapperOutKey implements
		WritableComparable<ModifyArtistMapperOutKey> {
	private Text id;
	private IntWritable tag; // 用作二次排序分组

	public ModifyArtistMapperOutKey(int tag) {
		id = new Text();
		this.tag = new IntWritable(tag);
	}

	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		tag.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		id.write(out);
		tag.write(out);
	}

	// group
	public int compareTo(ModifyArtistMapperOutKey o) {

		return id.compareTo(o.id);
	}


	public Text getId() {
		return id;
	}

	public void setId(String id) {
		this.id = new Text(id);
	}

	public IntWritable getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = new IntWritable(tag);
	}
}
