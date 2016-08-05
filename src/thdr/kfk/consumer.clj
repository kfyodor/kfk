(ns thdr.kfk.consumer
  "A thin wrapper around Kafka Java Consumer (0.9-0.10)"
  (:require [thdr.kfk
             [util :as u]
             [types :refer [to-map]]]
            [schema.core :as s]
            [clojure.string :as str])
  (:import  [org.apache.kafka.clients.consumer KafkaConsumer Consumer]
            [org.apache.kafka.common.serialization Deserializer]))

(s/defschema ConsumerArgs
  {(s/optional-key :key-deserializer)   Deserializer
   (s/optional-key :value-deserializer) Deserializer
   :props u/Config})

(s/defschema TopicPartitions
  {(s/or s/Str s/Keyword) [s/Int]})

(defn- make-TopicPartition
  [^String topic ^int partition]
  (TopicPartition. topic partition))

(defn- make-TopicPartitions
  [topic-partitions]
  {:pre (s/validate TopicPartitions topic-partitions)}
  (mapcat (fn [[topic partitions]]
            (map (partial make-TopicPartition (name topic))
                 partitions))
          topic-partitions))

(defn assign!
  "Manually assign a list of partitions to consumer

   `topic-partition` should be a map with topics as keys
   and vectors of partition numbers as values
   (?????? or maybe just a collection of maps {:topic :partition} ???????)"
  [^Consumer consumer topic-partitions]
  {:pre (s/validate TopicPartitions topic-partitions)}
  (.assign consumer
           (make-TopicPartitions topic-partitions)))

(defn assignment
  [^Consumer consumer]
  (map to-map (.assignment consumer)))

(defn partition-info
  [^Consumer consumer ^String topic]
  (map to-map (.partitionInfo consumer topic)))

(defn commit-async!
  ([^Consumer consumer])
  ([^Consumer consumer callback-fn])
  ([^Consumer consumer offsets callback-fn]))

(defn commit-sync!
  ([^Consumer consumer])
  ([^Consumer consumer offsets]))

(defn commited
  ([^Consumer consumer
    ^String topic
    ^int partition]
   (.commited consumer (make-TopicPartition topic partition))))

(defn list-topics
  [^Consumer consumer]
  (into {} (map (fn [[topic p-infos]]
                  [topic (mapv to-map p-infos)])
                (.listTopics consumer))))

(defn metrics
  [^Consumer consumer])

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
  [^Consumer consumer
   ^String topic
   ^int partition]
  (.position consumer (make-TopicPartition topic partition)))

(defn resume!
  [^Consumer consumer topic-partitions]
  (.resume consumer
           (make-TopicPartitions topic-partitions)))

(defn seek!
  [^Consumer consumer
   ^String topic
   ^int partition
   ^long offset]
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
  (reify ConsomerRebalanceListener
    (onPartitionsAssigned [this topic-partitions]
      (-> (map to-map topic-partitions)
          (on-assigned-fn)))
    (onPartitionRevoked [this topic-partitions]
      (-> (map to-map topic-partitions)
          (on-revoked-fn)))))

(defn subscribe!
  "Subscribe to topics"
  ([^Consumer consumer topics]
   (subscribe! consumer topics nil))
  ([^Consumer consumer
    topics
    on-assigned-fn
    on-revoked-fn
    ;; ..... TODO
    ;(.subscribe consumer topics rebalance-listener-fn)
    ]))

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
  [^Consumer consumer
   ^int poll-timeout]
  (->> (iterator-seq (.iterator (.poll consumer poll-timeout)))
       (map to-map)))

(defn stream!
  "Makes a flat stream of Kafka messages.
   Commits before each poll when `:commit-before-next-poll`
   set to `true` (default is `true`)."
  ([^Consumer consumer]
   (stream consumer {}))
  ([^Consumer consumer
    {:keys [poll-timeout commit-prev commit-before-next-poll]
     :or {poll-timeout 3000
          commit-prev false
          commit-before-next-poll true}
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
  {:pre (s/validate ConsumerArgs args)}
  (-> (u/make-props props)
      (KafkaConsumer. key-deserializer value-deserializer)))
