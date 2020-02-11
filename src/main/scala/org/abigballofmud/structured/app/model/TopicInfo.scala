package org.abigballofmud.structured.app.model

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2020/02/11 11:14
 * @since 1.0
 */
case class TopicInfo(topic: String,
                     partition: String,
                     offset: String) extends Serializable
