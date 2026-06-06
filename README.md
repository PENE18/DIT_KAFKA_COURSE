# TP Kafka — Installation, Administration et Streaming avec Scala

**Objectif** : Deployer un cluster Apache Kafka avec Docker, maitriser les commandes CLI,
puis developper un pipeline de streaming complet en Scala (Producer → Topic → Consumer).

---

## Prerequis

| Outil | Version minimale | Verification |
|-------|-----------------|--------------|
| Docker | 24.x | `docker --version` |
| Docker Compose | 2.x | `docker compose version` |
| JDK | 11+ | `java -version` |
| SBT | 1.9.x | `sbt --version` |
| IntelliJ IDEA | 2023.x | avec le plugin Scala installe |

---

## Structure du Projet

```
kafka-tp/
├── build.sbt                             # Definition du projet SBT
├── project/
│   ├── build.properties                  # Version SBT
│   └── plugins.sbt                       # Plugin sbt-assembly (fat JAR)
├── docker-compose.yml                    # Cluster single-node (developpement)
├── docker-compose-cluster.yml            # Cluster 3 brokers (prod-like)
├── README.md
└── src/
    └── main/
        ├── scala/
        │   ├── models/
        │   │   └── Order.scala           # Modele de donnees
        │   ├── OrderProducer.scala       # Producer Kafka
        │   └── OrderConsumer.scala       # Consumer Kafka + point d'entree
        └── resources/
            └── logback.xml               # Configuration des logs
```

---

## Ouverture dans IntelliJ IDEA

1. Ouvrir IntelliJ IDEA
2. Choisir **File > Open**
3. Selectionner le dossier `kafka-tp/`
4. IntelliJ detecte automatiquement le fichier `build.sbt`
5. Cliquer **Open as Project** dans la boite de dialogue qui s'affiche
6. Attendre que SBT telecharge toutes les dependances (premiere fois : 2-5 minutes)
7. Verifier que le plugin **Scala** est installe : `File > Settings > Plugins > Scala`

Pour executer un programme depuis IntelliJ : clic droit sur `OrderProducer` ou `MainConsumer` → **Run**.

---

## Partie 1 — Installation du Cluster Kafka avec Docker

### 1.1 — Cluster single-node (developpement)

Le fichier `docker-compose.yml` est deja pret dans le projet. Demarrer le cluster :

```bash
docker compose up -d
```

Verifier que tous les services sont demarres :

```bash
docker compose ps
```

Resultat attendu :

```
NAME             IMAGE                                    STATUS
kafka            confluentinc/cp-kafka:7.6.0              Up
kafka-ui         provectuslabs/kafka-ui:latest            Up
schema-registry  confluentinc/cp-schema-registry:7.6.0   Up
```

Suivre les logs en temps reel :

```bash
docker compose logs -f kafka
```

Interface graphique disponible a l'adresse : http://localhost:8080

### 1.2 — Cluster 3 brokers (exercice avance)

```bash
docker compose -f docker-compose-cluster.yml up -d
```

---

## Partie 2 — Administration via CLI

Toutes les commandes s'executent depuis le terminal. La syntaxe `docker exec kafka` permet
d'appeler les outils CLI directement a l'interieur du conteneur.

### 2.1 — Gestion des topics

Creer le topic `orders` :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000
```

Note : `replication-factor 1` est adapte au single-node. Sur un cluster 3 brokers, utiliser `3`.

Lister les topics existants :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

Decrire un topic (partitions, leaders, repliques) :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic orders
```

Sortie attendue :

```
Topic: orders   PartitionCount: 3   ReplicationFactor: 1
  Partition: 0   Leader: 1   Replicas: 1   Isr: 1
  Partition: 1   Leader: 1   Replicas: 1   Isr: 1
  Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

Modifier la configuration d'un topic (changer la retention a 1 jour) :

```bash
docker exec kafka kafka-configs \
  --bootstrap-server localhost:9092 \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config retention.ms=86400000
```

Augmenter le nombre de partitions (operation irreversible) :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --alter --topic orders --partitions 6
```

Supprimer un topic :

```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete --topic orders
```

### 2.2 — Tester avec les outils console

Ouvrir un terminal Producer interactif :

```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property key.separator=: \
  --property parse.key=true
```

Taper des messages au format `cle:valeur` :

```
user-1:{"orderId": "ORD-001", "amount": 99.99, "status": "CREATED"}
user-2:{"orderId": "ORD-002", "amount": 149.50, "status": "CREATED"}
```

Ouvrir un second terminal Consumer :

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true \
  --property key.separator=" | "
```

Consumer dans un groupe nomme :

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group my-consumer-group \
  --from-beginning
```

### 2.3 — Gestion des consumer groups

Lister les groupes actifs :

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

Inspecter le lag d'un groupe :

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group my-consumer-group
```

Lecture de la sortie :

```
GROUP             TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
my-consumer-group orders  0          1000            1050            50
my-consumer-group orders  1          980             980             0
```

LAG = nombre de messages non encore traites pour cette partition.

Reinitialiser les offsets au debut (rejouer tous les messages) :

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --reset-offsets \
  --to-earliest \
  --topic orders \
  --execute
```

Reinitialiser a un instant precis :

```bash
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group my-consumer-group \
  --reset-offsets \
  --to-datetime 2024-01-01T00:00:00.000 \
  --topic orders \
  --execute
```

---

## Partie 3 — Pipeline Scala

### 3.1 — Modele de donnees : `models/Order.scala`

La classe `Order` represente une commande. Elle utilise Circe pour la serialisation JSON
et un ADT (`sealed trait OrderStatus`) pour les statuts possibles. Les encodeurs/decodeurs
sont derives automatiquement via `deriveEncoder` et `deriveDecoder`.

### 3.2 — Producer : `OrderProducer.scala`

Points cles de la configuration :

| Parametre | Valeur | Role |
|-----------|--------|------|
| `acks=all` | all | Attend la confirmation de tous les ISR |
| `enable.idempotence=true` | true | Empeche les doublons en cas de retry |
| `linger.ms=10` | 10 ms | Regroupe les messages en batch avant envoi |
| `compression.type=snappy` | snappy | Compresse les batches, reduit la bande passante |
| `batch.size=65536` | 64 KB | Taille max d'un batch avant envoi force |

La cle du message est le `userId`. Cela garantit que toutes les commandes du meme
utilisateur arrivent dans la meme partition, dans l'ordre.

### 3.3 — Consumer : `OrderConsumer.scala`

Points cles :

| Parametre | Valeur | Role |
|-----------|--------|------|
| `enable.auto.commit=false` | false | Commit manuel apres traitement |
| `auto.offset.reset=earliest` | earliest | Lire depuis le debut si pas d'offset connu |
| `max.poll.records=100` | 100 | Taille max du batch par poll() |
| `session.timeout.ms=30000` | 30 s | Delai avant qu'un consumer soit considere mort |

Strategiede commit : les offsets sont commites de facon synchrone apres chaque batch
entier traite avec succes. Si le programme crash en milieu de batch, les messages seront
re-traites au redemarrage (semantique at-least-once). Le handler doit donc etre idempotent.

---

## Partie 4 — Execution du Pipeline

### 4.1 — Preparer le topic

```bash
# Supprimer si deja existant
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete --topic orders 2>/dev/null || true

# Recreer proprement
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

### 4.2 — Compiler le projet

```bash
sbt compile
```

### 4.3 — Lancer le Consumer (Terminal 1)

Le consumer doit etre demarre avant le producer pour ne manquer aucun message.

```bash
sbt "runMain MainConsumer"
```

Sortie attendue :

```
12:00:01.234 [main] INFO  OrderConsumer - Abonne au topic 'orders' (groupe: order-processor-v1)
```

### 4.4 — Lancer le Producer (Terminal 2)

```bash
sbt "runMain OrderProducer"
```

Sortie attendue (Producer) :

```
12:00:05.100 [main] INFO  OrderProducer$ - Debut de l'envoi de 50 commandes...
12:00:05.312 [kafka-producer-...] INFO  OrderProducer$ - Message envoye -> orders [partition=0, offset=0]
12:00:05.900 [main] INFO  OrderProducer$ - 50 commandes envoyees avec succes.
```

Sortie attendue (Consumer) :

```
12:00:05.400 [main] INFO  OrderConsumer  - Batch recu : 50 messages
12:00:05.401 [main] INFO  MainConsumer$  - Commande recue : ORD-000001 | Utilisateur: USR-0001 | Montant: 12.5 EUR | Statut: Created
12:00:05.402 [main] INFO  MainConsumer$  -   -> Traitement de ORD-000001...
12:00:05.600 [main] INFO  OrderConsumer  - Committed 3 partition(s)
```

---

## Partie 5 — Exercices d'Approfondissement

### Exercice 1 — Observer le partitionnement

Demarrer 3 instances du Consumer dans 3 terminaux differents avec le meme `group.id`,
puis lancer le Producer. Observer comment Kafka distribue les partitions entre les instances.

```bash
# Terminal 1, 2 et 3 — meme group.id
sbt "runMain MainConsumer"
```

Question : combien de partitions recoit chaque instance ? Pourquoi ?

### Exercice 2 — Simuler un Consumer lent

Ajouter un `Thread.sleep(200)` dans le handler du Consumer. Surveiller le lag en parallele :

```bash
watch -n 2 "docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group order-processor-v1"
```

Question : que se passe-t-il si le lag augmente indefiniment ?

### Exercice 3 — Reset d'offsets et rejeu

Apres avoir consomme tous les messages, reinitialiser les offsets et observer le rejeu :

```bash
# Stopper le consumer (Ctrl+C), puis reinitialiser
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group order-processor-v1 \
  --reset-offsets \
  --to-earliest \
  --topic orders \
  --execute

# Relancer : tous les messages sont rejoues depuis le debut
sbt "runMain MainConsumer"
```

### Exercice 4 — Cluster multi-brokers

Demarrer le cluster 3 brokers et recrer le topic avec replication-factor 3 :

```bash
docker compose -f docker-compose-cluster.yml up -d

docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders-cluster \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

Arreter un broker et verifier que le cluster reste disponible :

```bash
docker stop kafka-2
```

### Exercice 5 — Comprendre les ISR

Apres avoir arrete `kafka-2`, inspecter l'etat des partitions :

```bash
docker exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic orders-cluster
```

Question : que signifie `Isr: 1,3` au lieu de `Isr: 1,2,3` ?
Qu'est-ce qui se passerait si `min.insync.replicas=2` et qu'il ne reste plus qu'un seul ISR ?

---

## Partie 6 — Arret et Nettoyage

```bash
# Arreter les conteneurs (donnees conservees)
docker compose down

# Arreter ET supprimer les volumes (donnees perdues)
docker compose down -v

# Lister les volumes Docker restants
docker volume ls | grep kafka
```

---

## Concepts Cles

| Concept | Definition |
|---------|-----------|
| Partition | Unite de parallelisme. Plus de partitions = plus de consumers en parallele |
| Offset | Position d'un message dans une partition. Identifiant unique et immuable |
| Consumer Group | Ensemble de consumers qui se partagent les partitions d'un topic |
| Lag | Nombre de messages non encore traites (LOG-END-OFFSET - CURRENT-OFFSET) |
| ISR | In-Sync Replicas : replicas a jour. Crucial pour la durabilite des donnees |
| Idempotence | Un meme message produit plusieurs fois n'a d'effet qu'une seule fois |
| At-least-once | Le consumer peut recevoir un message plusieurs fois mais ne le rate jamais |
| Commit manuel | Vous controlez quand un message est considere comme traite |

---

## References

- Documentation officielle Apache Kafka : https://kafka.apache.org/documentation/
- Confluent Platform Docker Images : https://hub.docker.com/r/confluentinc/cp-kafka
- kafka-clients API : https://kafka.apache.org/37/javadoc/
- Circe (JSON pour Scala) : https://circe.github.io/circe/
- Kafka UI : https://github.com/provectus/kafka-ui
