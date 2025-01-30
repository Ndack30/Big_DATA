
from kafka import KafkaProducer
import datetime as dt
import json
import random
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, from_json


def generate_transaction():
    transaction_types = ['achat', 'remboursement', 'transfert']
    payment_methods = ['carte_de_credit', 'especes', 'virement_bancaire', 'erreur']

    current_time = dt.datetime.now().isoformat()

    Villes = ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille", "Rennes", "Reims", "Le Havre", "Saint-Étienne", "Toulon" , None]
    Rues = ["Rue de la République", "Rue de Paris", "rue Auguste Delaune", "Rue Gustave Courbet ", "Rue de Luxembourg", "Rue Fontaine", "Rue Zinedine Zidane", "Rue de Bretagne", "Rue Marceaux", "Rue Gambetta", "Rue du Faubourg Saint-Antoine", "Rue de la Grande Armée", "Rue de la Villette", "Rue de la Pompe", "Rue Saint-Michel" , None]

    transaction_data = {
        "id_transaction": str(uuid.uuid4()),
        "type_transaction": random.choice(transaction_types),
        "montant": round(random.uniform(10.0, 1000.0), 2),
        "devise": "USD",
        "date": current_time,
        "lieu": f"{random.choice(Rues)}, {random.choice(Villes)}",
        "moyen_paiement": random.choice(payment_methods),
        "details": {
            "produit": f"Produit{random.randint(1, 100)}",
            "quantite": random.randint(1, 10),
            "prix_unitaire": round(random.uniform(5.0, 200.0), 2)
        },
        "utilisateur": {
            "id_utilisateur": f"User{random.randint(1, 1000)}",
            "nom": f"Utilisateur{random.randint(1, 1000)}",
            "adresse": f"{random.randint(1, 1000)} {random.choice(Rues)}, {random.choice(Villes)}",
            "email": f"utilisateur{random.randint(1, 1000)}@example.com"
        }
    }

    return transaction_data

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Adresse de votre serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialiser les données au format JSON
)

# Nom du topic Kafka
topic = 'transaction'  # Nom corrigé du topic

# Envoi continu des transactions au topic
while True:
    transaction = generate_transaction()  # Générer une transaction
    producer.send(topic, transaction)  # Envoyer la transaction à Kafka
    print(f"Transaction envoyée : {transaction}")  # Journalisation
    time.sleep(1)  # Pause de 1 seconde entre les envois


#Envoyer la data sur votre conducktor


#--------------------------------------------------------------------
#                     MANIP SUR LES DF
#--------------------------------------------------------------------
#       convertir USD en EUR
#       ajouter le TimeZone
#       remplacer la date en string en une valeur date
#       supprimer les transaction en erreur
#       supprimer les valeur en None ( Adresse )



# si ça vous gene faite mettez tout sur la meme partie ( en gros supprimer les sous structure pour tout mettre en 1er plan )
# modifier le consumer avant



# KAFKA PRODUCEUR
# Reception des donnes en Pyspark ou en Spark scala
# Manip
# Envoie dans un bucket Minio ou sur hadoop pour les plus téméraire



