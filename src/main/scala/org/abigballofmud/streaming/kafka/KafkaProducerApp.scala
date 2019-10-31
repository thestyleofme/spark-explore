package org.abigballofmud.streaming.kafka

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Properties, Random, UUID}

import com.google.gson.Gson
import org.abigballofmud.streaming.model.VinSourceData
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


/**
 * <p>
 * topic 消息发送
 * </p>
 *
 * @author isacc 2019/10/17 11:31
 * @since 1.0
 */
object KafkaProducerApp {

  private val gson = new Gson()

  def main(args: Array[String]): Unit = {

    val brokers = "hdsp001:6667,hdsp002:6667,hdsp003:6667"
    val topic = "vin-source"
    //    val brokers = "120.92.19.15:6667"
    //    val topic = "GW-GWM-uaes-topic"
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    // 创建生产者
    val producer = new KafkaProducer[String, String](props)
    // 发送数据

    // 车型
    val carType = "C80G|C31D|B60V|C70G|C60F".split("\\|")
    val province = "北京市|天津市|上海市|重庆市|河北省|河南省|云南省|辽宁省|黑龙江省|湖南省|安徽省|山东省|新疆维吾尔|江苏省|浙江省|江西省|湖北省|广西壮族|甘肃省|山西省|内蒙古|陕西省|吉林省|福建省|贵州省|广东省|青海省|西藏|四川省|宁夏回族|海南省|台湾省|香港特别行政区|澳门特别行政区".split("\\|")
    val city = "成都市|绵阳市|德阳市|攀枝花市|遂宁市|南充市|广元市|乐山市|宜宾市|泸州市|达州市|广安市|巴中市|雅安市|内江市|自贡市|资阳市|眉山市|武汉市|黄石市|十堰市|荆州市|宜昌市|襄阳市|鄂州市|荆门市|孝感市|黄冈市|咸宁市|随州市|长沙市|株洲市|湘潭市|岳阳市|益阳市|常德市|衡阳市|邵阳市|张家界市|郴州市|永州市|怀化市|娄底市|常州市|徐州市|南京市|淮安市|南通市|宿迁市|无锡市|扬州市|盐城市|苏州市|泰州市|镇江市|连云港市".split("\\|")
    // 信号指标：knkRatio-爆震因子，preIgnRatio-早燃因子，relAirCharge-负荷，etc
    val sts = "c_SuprKnk|c_HSPsecInjectionDyn|c_PreIgnDetct|engSpdGradt1cycl|filtGradtManifPres|preIgnRatio|knkRatio|ignAngle|engSpd|relAirCharge|1stPreignNum|2ndPreignNum|ambientTemp|3rdPreignNum4thPreignNum|intkAirTemp|drivingMileage|altudCorcFactor|carSpd|engTemp|relOilPres".split("\\|")
    // 是否爆震
    val knkFlags = Array("Y", "N")
    val random = new Random()

    while (true) {
      for (i <- 1 to 2) {
        if (i == 1) {
          val knkRatioData = VinSourceData(UUID.randomUUID.toString,
            "LNBSCCAK0JT" + random.nextInt(200),
            "event.suprKnkSrv.knkRatio",
            (Math.random() * 100).toString,
            System.currentTimeMillis().toString,
            carType.apply(random.nextInt(carType.length)),
            "GWM",
            random.nextInt(250).toString,
            sts.apply(random.nextInt(sts.length)),
            LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
            LocalDateTime.now().toString
          )
          producer.send(new ProducerRecord(topic, UUID.randomUUID.toString, gson.toJson(knkRatioData)))
        } else {
          val gpsData = VinSourceData(UUID.randomUUID.toString,
            "LNBSCCAK0JT" + random.nextInt(200),
            "gps.lat",
            (Math.random() * 100).toString,
            System.currentTimeMillis().toString,
            carType.apply(random.nextInt(carType.length)),
            "GWM",
            random.nextInt(250).toString,
            sts.apply(random.nextInt(sts.length)),
            LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
            LocalDateTime.now().toString
          )
          producer.send(new ProducerRecord(topic, UUID.randomUUID.toString, gson.toJson(gpsData)))
        }
        println("发送一条~", LocalDateTime.now())
        Thread.sleep(random.nextInt(10) * 1000)
      }
    }
    // 关闭
    producer.close()
  }

}
