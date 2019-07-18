package cn.beginman.flinkapp.util

case class UserBehavior(userId: Long,   // 用户ID
                        itemId: String,   // 条目ID
                        categoryId: Int, // 类别ID
                        behavior: String, // 用户行为，包括("pv", "buy", "cart", "fav")
                        timestamp: Long)
