package com.wxy.spark.FastCD

class NodeData(val nodeID: Long, var communityID: Long) extends Serializable {
  var communityDegreeSum: Long = 0
  var Q: Double = 0.0
  var isUpdate: Boolean = false
  var neighCommunityDegreeSum: Long = 0

  override def toString: String =
    "nodeid =>" + nodeID + " communityDegreeSum=>" + communityDegreeSum + " q => " + Q + " communityID " +
      communityID + " update " + isUpdate + " neighCommunityDegreeSum => " + neighCommunityDegreeSum

}
