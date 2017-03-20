package cn.cslg.modifydata.parser;

import org.apache.hadoop.io.Text;

/**
 * parse file user_artist_data
 * @author xwtech
 */
public class UserArtistDataParser {
	private String userid;
	private String artistid;
	private int playcount;
	
	/**
	 * split by '\\s'
	 * userid artistid playcount
	 * @param line
	 * @return 
	 */
	public boolean parse(String line) {
		String[] splits = line.split("\t");
		if (splits == null || splits.length != 3) {
			return false;
		}
		userid = splits[0].trim();
		artistid = splits[1].trim();
		playcount = Integer.parseInt(splits[2].trim());
		return true;
	}
	
	public boolean parse(Text line) {
		return parse(line.toString());
	}
	
	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getArtistid() {
		return artistid;
	}

	public void setArtistid(String artistid) {
		this.artistid = artistid;
	}

	public int getPlaycount() {
		return playcount;
	}

	public void setPlaycount(int playcount) {
		this.playcount = playcount;
	}
}
