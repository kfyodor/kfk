(ns thdr.kfk.consumer
  "A thin wrapper around Kafka Java Consumer (0.9-0.10)"
  (:require [thdr.kfk
             [util :as u]
             [types :refer [to-map]]]
            [schema.core :as s]
            [clojure.string :as str])
  (:import [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.clients.consumer
            KafkaConsumer
            Consumer
            ConsumerRebalanceListener
            OffsetAndMetadata]
           [org.apache.kafka.clients.consumer.internals
            NoOpConsumerRebalanceListener]
           [org.apache.kafka.common.serialization Deserializer]))

;; TODO: commitAsync and docstrings

(s/defschema ConsumerArgsSchema
  {(s/optional-key :key-deserializer)   Deserializer
   (s/optional-key :value-deserializer) Deserializer
   :props u/PropsMap})

(s/defschema OffsetsSchema
  [{:topic s/Str
    :partition s/Int
    :offset s/Int
    (s/optional-key :metadata) s/Str}])

(s/defschema TopicPartitionSchema
  {:topic s/Str
   :partition s/Int})

(s/defschema TopicPartitionsSchema
  [TopicPartitionSchema])

(defn- make-TopicPartition
  [^String topic partition]
  (TopicPartition. topic partition))

(defn- make-TopicPartitions
  [topic-partitions]
  {:pre (s/validate TopicPartitionsSchema topic-partitions)}
  (->> topic-partitions
       (map (fn [{:keys [topic partition]}]
              (make-TopicPartition topic partition)))))

(defn- make-Offsets
  [offsets]
  (->> offsets
       (map (fn [{:keys [topic partition offset metadata]
                 :or {metadata ""}}]
              [(make-TopicPartition topic partition)
               (OffsetAndMetadata. offset metadata)]))
       (into {})))

;;;;;;;;;; PUBLIC API ;;;;;;;;;;

(defn assign!
  "Manually assign a list of topics and
   partitions to consumer.

   `topic-partitions` should be a collection
   of maps with keys `:topic` and `:partition`"
  [^Consumer consumer topic-partitions]
  (.assign consumer
           (make-TopicPartitions topic-partitions)))

(defn assignment
  "Get current assigned topics
   and partitions for consumer."
  [^Consumer consumer]
  (map to-map (.assignment consumer)))

(defn commit-async!
  ([^Consumer consumer])
  ([^Consumer consumer callback-fn])
  ([^Consumer consumer offsets callback-fn]))

(defn commit-sync!
  "Synchronously commits offsets for all topics and
   partitions fetched via poll! if no specific
   `offsets` are specified.

   `offsets` must be a collection of maps with keys:
   `:topic`, `:partition`, `:offset` and optional
   `:metadata` key."
  ([^Consumer consumer]
   (.commitSync consumer))
  ([^Consumer consumer offsets]
   (.commitSync consumer (make-Offsets offsets))))

(defn committed
  "Get latest committed offsets for
   specified topic and partition."
  ([^Consumer consumer ^String topic partition]
   (to-map
    (.committed consumer (make-TopicPartition topic partition)))))

(defn list-topics
  "List topics consumer subscribed to."
  [^Consumer consumer]
  (into {} (map (fn [[topic p-infos]]
                  [topic (mapv to-map p-infos)])
                (.listTopics consumer))))

(defn metrics
  [^Consumer consumer]
  :TODO)

(defn partitions-for
  [^Consumer consumer ^String topic]
  (mapv to-map
        (.partitionsFor consumer topic)))

(defn pause!
  [^Consumer consumer topic-partitions]
  (.pause consumer
          (make-TopicPartitions topic-partitions)))

(defn paused
  [^Consumer consumer]
  (mapv to-map (.paused consumer)))

(defn position
  [^Consumer consumer ^String topic partition]
  (.position consumer (make-TopicPartition topic partition)))

(defn resume!
  [^Consumer consumer topic-partitions]
  (.resume consumer
           (make-TopicPartitions topic-partitions)))

(defn seek!
  [^Consumer consumer ^String topic partition offset]
  (.seek consumer
         (make-TopicPartition topic partition)
         offset))

(defn seek-to-beginning!
  [^Consumer consumer topic-partitions]
  (.seekToBeginning consumer
                    (make-TopicPartitions topic-partitions)))

(defn seek-to-end!
  [^Consumer consumer topic-partitions]
  (.seekToEnd consumer
              (make-TopicPartitions topic-partitions)))

(defn make-rebalance-listener
  [on-assigned-fn on-revoked-fn]
  (reify ConsumerRebalanceListener
    (onPartitionsAssigned [this topic-partitions]
      (-> (map to-map topic-partitions)
          (on-assigned-fn)))
    (onPartitionsRevoked [this topic-partitions]
      (-> (map to-map topic-partitions)
          (on-revoked-fn)))))

(defn subscribe!
  "Subscribe to topics"
  ([^Consumer consumer topics]
   (.subscribe consumer topics (NoOpConsumerRebalanceListener.)))
  ([^Consumer consumer topics ^ConsumerRebalanceListener listener]
   (.subscribe consumer topics listener))
  ([^Consumer consumer topics on-assigned-fn on-revoked-fn]
   (.subscribe consumer
               topics
               (make-rebalance-listener on-assigned-fn
                                        on-revoked-fn))))

(defn subscription
  "Get set of topics consumer subscribed to."
  [^Consumer consumer]
  (into #{} (.subscription consumer)))

(defn unsubscribe!
  [^Consumer consumer]
  (.unsubscribe consumer))

(defn wakeup!
  [^Consumer consumer]
  (.wakeup consumer))

(defn poll!
  "Makes a stream of ConsumerRecord bathces
   returned from each poll. Doesn't commit offsets,
   it should be done manually."
  [^Consumer consumer poll-timeout]
  (->> (iterator-seq (.iterator (.poll consumer poll-timeout)))
       (map to-map)))

(defn stream!
  "Makes a flat stream of Kafka messages.
   Commits before each poll when `:commit-before-next-poll`
   set to `true` (default is `false`)."
  ([^Consumer consumer]
   (stream! consumer {}))
  ([^Consumer consumer
    {:keys [poll-timeout commit-prev commit-before-next-poll]
     :or {poll-timeout 3000
          commit-prev false
          commit-before-next-poll false}
     :as opts}]
   (when (and commit-before-next-poll
              commit-prev)
     (.commitSync consumer))
   (lazy-cat (poll! consumer poll-timeout)
             (lazy-seq (stream! consumer
                                (assoc opts :commit-prev true))))))


(defn kafka-consumer
  "Makes an instance of KafkaConsumer
   TODO: rebalance listener and stuff"
  [& {:keys [key-deserializer value-deserializer props] :as args}]
  {:pre (s/validate ConsumerArgsSchema args)}
  (-> (u/make-props props)
      (KafkaConsumer. key-deserializer value-deserializer)))
