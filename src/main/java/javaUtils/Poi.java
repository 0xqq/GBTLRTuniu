package javaUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Poi implements Comparable<Object>, Serializable, Cloneable {
	private static final long serialVersionUID = 1L;
	private int poiid;
	private String name;
	private Integer type;

	// 可变变量
	private float prefer; // 推荐的权重
	private String tag;
	private int isFollow = 0; // 未关注

	private Map<Integer, Poi> children;

	/**
	 * 功能描述：<br>
	 * 默认构造函数
	 */
	public Poi() {
		super();
	}

	/**
	 * 构造方法
	 * 
	 * @param pid
	 * @param pname
	 * @param poitype
	 * @param poilevel
	 * @param tagName
	 * @param tagWeight
	 */
	public Poi(int pid, String pname, Integer poitype, String tagName, float weight) {
		this.poiid = pid;
		this.name = pname;
		this.type = poitype;
		// 初始化偏好
		this.tag = tagName;
		this.prefer = weight;
		children = new HashMap<>();
	}

	public Map<Integer, Poi> fetchChildren() {
		return children;
	}

	public int compareTo(Object obj) {

		Poi another = (Poi) obj;
		int num = new Float(another.prefer).compareTo(new Float(this.prefer));
		return num == 0 ? this.name.compareTo(another.name) : num;

		/*
		 * if(this.age>stu.age) return 1; if(this.age==stu.age) return
		 * this.name.compareTo(stu.name); return -1;
		 */
	}

	public Object clone() {
		Poi o = null;
		try {
			o = (Poi) super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}
		return o;
	}

	public int getPoiid() {
		return poiid;
	}

	public void setPoiid(int poiid) {
		this.poiid = poiid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

//	public Integer getType() {
//		return type;
//	}
//
//	public void setType(Integer type) {
//		this.type = type;
//	}

	public float getPrefer() {
		return prefer;
	}

	public void setPrefer(float prefer) {
		this.prefer = prefer;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getIsFollow() {
		return isFollow;
	}

	public void setIsFollow(int isFollow) {
		this.isFollow = isFollow;
	}
}
