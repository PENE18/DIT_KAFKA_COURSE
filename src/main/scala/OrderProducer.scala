import com.typesafe.scalalogging.LazyLogging
import io.circe.syntax._
import models.{Order, OrderStatus}
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object OrderProducer extends App with LazyLogging {

  // ---------------------------------------------------------------------------
  // Configuration du Producer
  // ---------------------------------------------------------------------------
  def buildProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val props = new Properties()

    // Connexion
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,              bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,           classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,         classOf[StringSerializer].getName)

    // Fiabilite (at-least-once -> exactly-once avec idempotence)
    props.put(ProducerConfig.ACKS_CONFIG,                                    "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,                      "true")
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,          "5")
    props.put(ProducerConfig.RETRIES_CONFIG,                                 Integer.MAX_VALUE.toString)
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,                        "100")

    // Performance (batching)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG,       "65536")       // 64 KB par batch
    props.put(ProducerConfig.LINGER_MS_CONFIG,        "10")          // attendre 10ms avant envoi
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,    "33554432")    // 32 MB buffer local

    new KafkaProducer[String, String](props)
  }

  // ---------------------------------------------------------------------------
  // Envoi asynchrone avec Future
  // ---------------------------------------------------------------------------
  def sendAsync(
    producer: KafkaProducer[String, String],
    topic:    String,
    key:      String,
    value:    String
  ): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    val record  = new ProducerRecord[String, String](topic, key, value)

    producer.send(record, (metadata: RecordMetadata, exception: Exception) => {
      if (exception != null) {
        logger.error(s"Echec envoi [key=$key] : ${exception.getMessage}")
        promise.failure(exception)
      } else {
        logger.info(
          s"Message envoye -> ${metadata.topic()}" +
          s" [partition=${metadata.partition()}, offset=${metadata.offset()}]"
        )
        promise.success(metadata)
      }
    })

    promise.future
  }

  // ---------------------------------------------------------------------------
  // Programme principal
  // ---------------------------------------------------------------------------
  import scala.concurrent.ExecutionContext.Implicits.global

  val BOOTSTRAP_SERVERS = "localhost:9092"
  val TOPIC             = "orders"
  val NUM_ORDERS        = 50

  val producer = buildProducer(BOOTSTRAP_SERVERS)

  logger.info(s"Debut de l'envoi de $NUM_ORDERS commandes...")

  // Generer et envoyer les commandes
  val futures = (1 to NUM_ORDERS).map { i =>
    val order = Order(
      orderId  = f"ORD-$i%06d",
      userId   = f"USR-${i % 10}%04d",  // 10 utilisateurs differents
      amount   = BigDecimal(10.0 + i * 2.5).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
      currency = "EUR",
      status   = if (i % 5 == 0) OrderStatus.Processing else OrderStatus.Created
    )

    // La cle (userId) determine la partition -> garantit l'ordre par utilisateur
    sendAsync(producer, TOPIC, order.userId, order.asJson.noSpaces)
  }

  // Attendre tous les acquittements
  val results = Future.sequence(futures)
  results.onComplete {
    case Success(_)  => logger.info(s"$NUM_ORDERS commandes envoyees avec succes.")
    case Failure(ex) => logger.error(s"Erreur : ${ex.getMessage}")
  }

  Await.result(results, 30.seconds)

  producer.flush()
  producer.close()
  logger.info("Producer ferme proprement.")
}
