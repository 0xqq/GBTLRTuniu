package org.apache.spark.examples.ml

import org.apache.hadoop.hbase.client.{Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable

class readHBase {
  /**
    * 取用户最近与poi的交互特征
    * @param vt visitor_trace
    * @param poiid
    * @param vtPoiTab   user poi交互表
    * @return  user poi交互特征
    */
  def getUserPoiFeat(vt: String,poiid:String,vtPoiTab:Table) = {



  }


  /**
    *
    * @param poi
    * @param poiTable  poi离线数据HBase表
    * @return  poi离线特征
    */
  def getPoiFeats(poi: String, poiTable: Table):Map[String,Any] = {
    val poiOfFeat= mutable.Map(
      "poi_level"->0d,"country_flag"->"", "sum_pv_1"->0d,"sum_pv_2"->0d,"sum_pv_3"->0d,
      "sum_pv_4"->0d,"sum_pv_5"->0d, "sum_pv_6"->0d,"sum_pv_7"->0d,"poi_price"->0d,
      "poi_outdays"->0d,"poi_loccity"->"", "poi_locprovince"->"","poi_locregion"->"",
      "poi_loccountry"->"","poipref_1"->0d, "poipref_2"->0d,"poipref_3"->0d,"poipref_4"->0d,
      "poipref_5"->0d,"poipref_6"->0d, "poipref_7"->0d,"poipref_8"->0d,"poipref_9"->0d,
      "poipref_10"->0d,"poipref_11"->0d, "total_person_num"->0d,"last_person_num"->0d,
      "last_month_person_num"->0d,"last_year_person_num"->0d, "itf_poi_price"->0d,"poi_locprovince"->0d
    )
    val g = new Get(poi.getBytes)
    val result = poiTable.get(g)
    poiOfFeat("poi_level") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("country_flag") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_1") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_2") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_3") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_4") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_5") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_6") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("sum_pv_7") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_price") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_outdays") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_loccity") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_locprovince") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_locregion") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_loccountry") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_1") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_2") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_3") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_4") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_5") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_6") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_7") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_8") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_9") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_10") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poipref_11") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("total_person_num") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("last_person_num") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("last_month_person_num") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("last_year_person_num") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("itf_poi_price") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat("poi_locprovince_process") = Bytes.toString(result.getValue("data".getBytes,"country_flag".getBytes()))
    poiOfFeat.toMap
  }



}
