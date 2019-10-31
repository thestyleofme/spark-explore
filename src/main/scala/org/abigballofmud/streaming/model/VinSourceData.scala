package org.abigballofmud.streaming.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/18 16:16
 * @since 1.0
 */
case class VinSourceData(data_id: String,
                         vin: String,
                         st: String,
                         value: String,
                         ts: String,
                         ct: String,
                         oem: String,
                         rc: String,
                         sts: String,
                         ts_time: String,
                         creation_date: String,
                         province: String = null,
                         city: String = null,
                         knk_flag: String = "N") extends Serializable

/**
 * {
 * "val": "0.0",
 * "st": "rolling.engTrq",
 * "rc": "112",
 * "ct": "GWM",
 * "oem": "GW",
 * "vin": "LGW12345678900022",
 * "ts": 1571639105009
 * }
 * {
 * "st": "gps.lat",
 * "vin": "LGW12345678900022",
 * "val": "38.828674183333334",
 * "ts": "1571639137009000",
 * "ct": "GWM",
 * "oem": "GW"
 * }
 * {
 * "st": "gps.lon",
 * "vin": "LGW12345678900022",
 * "val": "115.45277938333334",
 * "ts": "1571639137009000",
 * "ct": "GWM",
 * "oem": "GW"
 * }
 */
class SourceData extends Serializable {
  var data_id: String = UUID.randomUUID().toString
  var vin: String = _
  var st: String = _
  var value: String = _
  var ts: String = _
  var ct: String = _
  var oem: String = _
  var rc: String = _
  var sts: String = _
  var ts_time: String = _
  var creation_date: String = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
}