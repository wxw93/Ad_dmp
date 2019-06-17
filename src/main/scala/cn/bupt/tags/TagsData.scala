package cn.bupt.tags

trait TagsData {
  /*
  * 定义打标签的方法
  * */
  def MakeTag(arg:Any*):Map[String,Int]

}
