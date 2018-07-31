package javaUtils;

import java.util.ArrayList;
import java.util.List;

public class UserRecord {
	private String uid;
	private String vt;
	private String act;
	private Long ts;
	private String uniqueId;
	private String bookCityCode;

	private List<Label> labs = new ArrayList<>();

	public UserRecord(String uid, String vt, String act, Long ts, String uniqueId, String bookCityCode) {
		this.setUid(uid);
		this.setVt(vt);
		this.setAct(act);
		this.setTs(ts);
		this.labs = new ArrayList<>();
		this.setUniqueId(uniqueId);
		this.setBookCityCode(bookCityCode);
	}

	public String getBookCityCode() {
		return bookCityCode;
	}

	public void setBookCityCode(String bookCityCode) {
		this.bookCityCode = bookCityCode;
	}

	public void addLabel(Label lab){
		this.labs.add(lab);
	}

	public UserRecord() {

	}

	public Long getTs() {
		return ts;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getVt() {
		return vt;
	}

	public void setVt(String vt) {
		this.vt = vt;
	}

	public String getAct() {
		return act;
	}

	public void setAct(String act) {
		this.act = act;
	}

	public List<Label> getLabs() {
		return labs;
	}

	public void setLabs(List<Label> labs) {
		this.labs = labs;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
}
