package javaUtils;

/**
 * 
 * @author zhucunliang
 * 区别于UserFeature，只关注特征本身的内容，类别与权重
 * 在主题推荐中，原始日志提取特征，标准词库表向上得到血脉，通过Feature只记录结点权重
 * 无需引入children属性：在向下折叠时才应考虑
 */
public class Feature implements Comparable<Object>{
	private String cate;// super category
	private String sc; // sub_category
	// private String pf; // parent feature
	private String feat;// feature
//	private Long ts;// last time
	/**
	 * 用户下特征的权重
	 * */
	private float valf;// 用户下特征的权重
	private float valu;// 特征下用户的权重

	public Feature(String cate, String sc, String fea) {
		this.cate = cate;
		this.sc = sc;
		// this.pf = pf;
		this.feat = fea;
//		this.ts = ts;
	}

	public Feature() {

	}
	
	public int compareTo(Object obj) {

		Feature another = (Feature) obj;
		int num = new Float(another.valf).compareTo(new Float(this.valf));
		return num == 0 ? this.feat.compareTo(another.feat) : num;

		/*
		 * if(this.age>stu.age) return 1; if(this.age==stu.age) return
		 * this.name.compareTo(stu.name); return -1;
		 */
	}

	public String takeOnlyId() {
		return sc + "_" + feat;
	}

	public String getFeat() {
		return feat;
	}

	public void setFeat(String feat) {
		this.feat = feat;
	}

	public String getSc() {
		return sc;
	}

	public void setSc(String sc) {
		this.sc = sc;
	}

	public String getCate() {
		return cate;
	}

	public void setCate(String cate) {
		this.cate = cate;
	}

	/**
	 * 获取特征的权重（一个用户下不同特征的权重）
	 * @return
	 */
	public float getValf() {
		return valf;
	}

	public void setValf(float valf) {
		this.valf = valf;
	}


	public float getValu() {
		return valu;
	}

	public void setValu(float valu) {
		this.valu = valu;
	}
}
