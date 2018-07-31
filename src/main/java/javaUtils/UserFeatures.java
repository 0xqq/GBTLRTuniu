package javaUtils;

import java.util.ArrayList;
import java.util.List;

public class UserFeatures {
	private String uid;
	private String vt;
	private Long ts;
	private String uniqueId;

	private List<Feature> feats = new ArrayList<>();

	public UserFeatures(String uid, String vt, Long ts, List<Feature> features, String uniqueId) {
		this.setUid(uid);
		this.setVt(vt);
		this.setTs(ts);
		this.feats = features;
		this.uniqueId = uniqueId;
	}

	public UserFeatures() {

	}

	public List<Feature> getFeats() {
		return feats;
	}

	public void setFeats(List<Feature> feats) {
		this.feats = feats;
	}

	public String getVt() {
		return vt;
	}

	public void setVt(String vt) {
		this.vt = vt;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
}
