import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import com.google.gson.Gson
import org.apache.kafka.common.serialization.{Serializer, StringSerializer}
import java.util.Random

import scala.concurrent.duration._
import scala.util.Random

case class Transaction(
                        idTransaction: String,
                        typeTransaction: String,
                        montant: Double,
                        devise: String,
                        date: String,
                        lieu: String,
                        moyenPaiement: Option[String],
                        details: Map[String, Any],
                        utilisateur: Map[String, Any]
                      )

class JsonSerializer[T] extends Serializer[T] {
  private val gson = new Gson

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = gson.toJson(data).getBytes

  override def close(): Unit = {}
}

object ScalaKafkaProducer extends App {
  implicit val system: ActorSystem = ActorSystem("ScalaKafkaProducer")

  val bootstrapServers = "localhost:9092"
  val topic = "transaction"

  val producerSettings =
    ProducerSettings(system, new StringSerializer, new JsonSerializer[Transaction])
      .withBootstrapServers(bootstrapServers)

  val paymentMethods = Seq("carte_de_credit", "especes", "virement_bancaire", null)
  val cities = Seq("Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon", null)
  val streets = Seq("Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet ", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel", null)

  def generateTransaction(): Transaction = {
    val transactionTypes = Seq("achat", "remboursement", "transfert")

    val currentDateTime = java.time.LocalDateTime.now().toString

    Transaction(
      idTransaction = UUID.randomUUID().toString,
      typeTransaction = util.Random.shuffle(transactionTypes).head,
      montant = 10.0 + util.Random.nextDouble() * (1000.0 - 10.0),
      devise = "USD",
      date = currentDateTime,
      lieu = s"${util.Random.shuffle(cities).headOption.getOrElse("")}, ${util.Random.shuffle(streets).headOption.getOrElse("")}",
      moyenPaiement = Option(util.Random.shuffle(paymentMethods).head),
      details = Map(
        "produit" -> s"Produit${util.Random.nextInt(100)}",
        "quantite" -> util.Random.nextInt(10),
        "prixUnitaire" -> util.Random.nextInt(200),
      ),
      utilisateur = Map(
        "idUtilisateur" -> s"User${util.Random.nextInt(1000)}",
        "nom" -> s"Utilisateur${util.Random.nextInt(1000)}",
        "adresse" -> s"${util.Random.nextInt(1000)} ${util.Random.shuffle(streets).headOption.getOrElse("")}, ${util.Random.shuffle(cities).headOption.getOrElse("")}",
        "email" -> s"utilisateur${util.Random.nextInt(1000)}@example.com"
      )
    )
  }

  val transactionsSource = Source.tick(0.seconds, 1.second, ())
    .map(_ => generateTransaction())

  val producerSink = Producer.plainSink(producerSettings)

  transactionsSource
    .map(transaction => new ProducerRecord[String, Transaction](topic, transaction.idTransaction, transaction))
    .runWith(producerSink)
}
