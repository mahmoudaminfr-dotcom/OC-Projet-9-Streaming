import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuration du producteur (connexion au broker Redpanda)
producer = KafkaProducer(
    bootstrap_servers=['redpanda_broker:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'client_tickets'

# Listes pour la génération aléatoire
TICKET_TYPES = ['Bug', 'Demande de fonctionnalité', 'Question technique', 'Problème de facturation']
TEAMS = ['Support N1', 'Support N2', 'DevOps', 'Facturation']
STATUSES = ['Nouveau', 'En cours', 'Résolu']

def generate_ticket():
    return {
        'ticket_id': random.randint(10000, 99999),
        'client_id': random.randint(100, 999),
        'timestamp': datetime.utcnow().isoformat(),
        'ticket_type': random.choice(TICKET_TYPES),
        'team_assigned': random.choice(TEAMS),
        'status': random.choice(STATUSES)
    }

if __name__ == "__main__":
    print(f"Début de l'envoi des données vers le topic '{TOPIC_NAME}'...")
    try:
        while True:
            ticket = generate_ticket()
            producer.send(TOPIC_NAME, ticket)
            print(f"Ticket envoyé : {ticket}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Arrêt du producteur.")
    finally:
        producer.close()
