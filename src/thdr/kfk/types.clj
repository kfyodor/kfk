(ns thdr.kfk.types
  (:import [org.apache.kafka.common
            TopicPartition
            PartitionInfo
            Node]

           [org.apache.kafka.clients.producer
            RecordMetadata
            ProducerRecord]

           [org.apache.kafka.clients.consumer
            OffsetAndMetadata
            ConsumerRecord
            ConsumerRecords]))

(defprotocol KafkaObjectToMap
  (to-map [this]))

(extend-protocol KafkaObjectToMap
  TopicPartition
  (to-map [this]
    {:topic (.topic this)
     :partition (.partition this)})

  OffsetAndMetadata
  (to-map [this]
    {:offset (.offset this)
     :metadata (.metadata this)})

  PartitionInfo
  (to-map [this]
    {:topic (.topic this)
     :partition (.partition this)
     :leader (to-map (.leader this))
     :replicas (mapv to-map (.replicas this))
     :in-sync-replicas (mapv to-map (.inSyncReplicas this))})

  Node
  (to-map [this]
    {:id (.idString this)
     :host (.host this)
     :port (.port this)
     :rack (.rack this)})

  RecordMetadata
  (to-map [this]
    {:checksum (.checksum this)
     :offset (.offset this)
     :partition (.partition this)
     :serialized-key-size (.serializedKeySize this)
     :serialized-value-size (.serializedValueSize this)
     :timestamp (.timestamp this)
     :topic (.topic this)})

  ConsumerRecord
  (to-map [this]
    {:key       (.key this)
     :offset    (.offset this)
     :partition (.partition this)
     :timestamp (.timestamp this) ;; TODO: Check if it works
     :topic     (.topic this)     ;; ..... with 0.9 since
     :value     (.value this)})   ;; ..... timestamps were added in 0.10.
  )
