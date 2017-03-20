package cn.cslg.modifydata.parser;

import org.apache.hadoop.io.Text;

/**
 * parse file artist_alias
 * 
 * @author xwtech
 */
public class ArtistAliasParser {
	private String badid;
	private String goodid;

	/**
	 * split by '\t' 
	 * badid goodid parser
	 * 
	 * @param line
	 * @return
	 */
	public boolean parse(String line) {
		String[] splits = line.split("\t");
		if (splits == null || splits.length != 2) {
			return false;
		}
		badid = splits[0].trim();
		goodid = splits[1].trim();
		return true;
	}

	public boolean parse(Text line) {
		return parse(line.toString());
	}

	public String getBadid() {
		return badid;
	}

	public void setBadid(String badid) {
		this.badid = badid;
	}

	public String getGoodid() {
		return goodid;
	}

	public void setGoodid(String goodid) {
		this.goodid = goodid;
	}
}
