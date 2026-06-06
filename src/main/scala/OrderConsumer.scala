import com.typesafe.scalalogging.LazyLogging
import io.circe.parser._
import models.{Order, OrderStatus}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

class OrderConsumer(bootstrapServers: String, groupId: String) extends LazyLogging {

  // ---------------------------------------------------------------------------
  // Configuration du Consumer
  // ---------------------------------------------------------------------------
  private val consumer: KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG,                  groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    // Commit manuel = controle total sur la livraison (at-least-once)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,   "false")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,    "earliest")

    // Timeouts et session
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,   "30000")
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,"10000")
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,     "100")

    // Fetch tuning
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,      "1024")
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,    "500")

    new KafkaConsumer[String, String](props)
  }

  @volatile private var running = true

  // Shutdown hook : wakeup() interrompt le poll() bloquant proprement
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    logger.info("Signal d'arret recu, fermeture en cours...")
    running = false
    consumer.wakeup()
  }))

  // ---------------------------------------------------------------------------
  // Boucle principale de consommation
  // ---------------------------------------------------------------------------
  def run(topic: String)(handler: Order => Unit): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    logger.info(s"Abonne au topic '$topic' (groupe: $groupId)")

    try {
      while (running) {
        val records = consumer.poll(Duration.ofMillis(500))

        if (!records.isEmpty) {
          logger.info(s"Batch recu : ${records.count()} messages")

          // Accumuler les offsets a committer apres traitement du batch entier
          val offsets = scala.collection.mutable.Map[TopicPartition, OffsetAndMetadata]()

          records.asScala.foreach { record =>
            logger.debug(
              s"  partition=${record.partition()}, offset=${record.offset()}, key=${record.key()}"
            )

            // Deserialisation JSON -> Order
            decode[Order](record.value()) match {
              case Right(order) =>
                try {
                  handler(order)
                  val tp = new TopicPartition(record.topic(), record.partition())
                  offsets(tp) = new OffsetAndMetadata(record.offset() + 1)
                } catch {
                  case e: Exception =>
                    logger.error(s"Erreur traitement @ offset ${record.offset()}: ${e.getMessage}")
                    throw e  // re-propager -> arret du consumer
                }

              case Left(error) =>
                // Poison pill : message non deserialisable -> log et skip
                logger.warn(s"Message invalide @ offset ${record.offset()} : $error")
                val tp = new TopicPartition(record.topic(), record.partition())
                offsets(tp) = new OffsetAndMetadata(record.offset() + 1)
            }
          }

          // Commit synchrone apres traitement du batch (at-least-once)
          if (offsets.nonEmpty) {
            consumer.commitSync(offsets.asJava)
            logger.info(s"Committed ${offsets.size} partition(s)")
          }
        }
      }

    } catch {
      case _: WakeupException =>
        logger.info("Consumer interrompu par wakeup()")
      case e: Exception =>
        logger.error(s"Erreur fatale : ${e.getMessage}", e)
        throw e
    } finally {
      consumer.close()
      logger.info("Consumer ferme proprement.")
    }
  }
}

// ---------------------------------------------------------------------------
// Point d'entree
// ---------------------------------------------------------------------------
object MainConsumer extends App with LazyLogging {

  val consumer = new OrderConsumer(
    bootstrapServers = "localhost:9092",
    groupId          = "order-processor-v1"
  )

  // Handler metier - doit etre idempotent !
  consumer.run("orders") { order =>
    logger.info(
      s"Commande recue : ${order.orderId} | " +
      s"Utilisateur: ${order.userId} | " +
      s"Montant: ${order.amount} ${order.currency} | " +
      s"Statut: ${order.status}"
    )

    // Logique metier selon le statut
    order.status match {
      case OrderStatus.Created    => logger.info(s"  -> Traitement de ${order.orderId}...")
      case OrderStatus.Processing => logger.info(s"  -> Commande ${order.orderId} deja en cours")
      case OrderStatus.Completed  => logger.info(s"  -> Commande ${order.orderId} terminee")
      case OrderStatus.Failed     => logger.warn(s"  -> Commande ${order.orderId} en echec !")
    }
  }
}
