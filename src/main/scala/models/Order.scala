package models

import java.time.Instant
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._

sealed trait OrderStatus
object OrderStatus {
  case object Created    extends OrderStatus
  case object Processing extends OrderStatus
  case object Completed  extends OrderStatus
  case object Failed     extends OrderStatus

  implicit val encoder: Encoder[OrderStatus] = Encoder.encodeString.contramap {
    case Created    => "CREATED"
    case Processing => "PROCESSING"
    case Completed  => "COMPLETED"
    case Failed     => "FAILED"
  }

  implicit val decoder: Decoder[OrderStatus] = Decoder.decodeString.emap {
    case "CREATED"    => Right(Created)
    case "PROCESSING" => Right(Processing)
    case "COMPLETED"  => Right(Completed)
    case "FAILED"     => Right(Failed)
    case other        => Left(s"Statut inconnu: $other")
  }
}

case class Order(
  orderId:   String,
  userId:    String,
  amount:    Double,
  currency:  String,
  status:    OrderStatus,
  timestamp: Long = Instant.now().toEpochMilli
)

object Order {
  implicit val encoder: Encoder[Order] = deriveEncoder[Order]
  implicit val decoder: Decoder[Order] = deriveDecoder[Order]
}
