package javaUtils;

import java.util.ArrayList;
import java.util.List;

public class Label {
	private String cate;//category
	private String sc;//subcategory
	private List<String> feats;//features
	private String lab;//label
	// private String modelType;

	public Label(String category, String subcategory, List<String> features, String label) {
		this.setCate(category);
		this.setSc(subcategory);
		this.setFeats(features);
		this.setLab(label);
	}
	public Label(){
		
	}

	public void addFeature(String feature) {
		if (feats == null) {
			feats = new ArrayList<>();
		}
		this.feats.add(feature);
	}
	public String getCate() {
		return cate;
	}
	public void setCate(String cate) {
		this.cate = cate;
	}
	public String getSc() {
		return sc;
	}
	public void setSc(String sc) {
		this.sc = sc;
	}
	public List<String> getFeats() {
		return feats;
	}
	public void setFeats(List<String> feats) {
		this.feats = feats;
	}
	public String getLab() {
		return lab;
	}
	public void setLab(String lab) {
		this.lab = lab;
	}

}
