(ns thdr.kfk.consumer
  "A thin wrapper around Kafka Java Consumer (0.9-0.10)"
  (:require [thdr.kfk.util :as u]
            [schema.core :as s]
            [clojure.string :as str])
  (:import  [org.apache.kafka.clients.consumer
             KafkaConsumer
             ConsumerRecord
             ConsumerRecords]
            [org.apache.kafka.common.serialization Deserializer]
            [java.util Properties]))

(s/defschema ConsumerArgs
  {(s/optional-key :key-deserializer)   Deserializer
   (s/optional-key :value-deserializer) Deserializer
   :props u/Config})

(defn- ConsumerRecord->map [^ConsumerRecord record]
  {:key       (.key record)
   :offset    (.offset record)
   :partition (.partition record)
   :timestamp (.timestamp record) ;; TODO: Check if it works
   :topic     (.topic record)     ;; ..... with 0.9 since
   :value     (.value record)})   ;; ..... timestamps were added in 0.10.

(defn subscribe!
  "Subscribe to topics"
  [^KafkaConsumer consumer topics]
  (doto consumer
    (.subscribe topics)))

(defn poll!
  "Makes a stream of ConsumerRecord bathces
   returned from each poll. Doesn't commit offsets,
   it should be done manually."
  [^KafkaConsumer consumer
   ^int poll-timeout]
  (->> (iterator-seq (.iterator (.poll consumer poll-timeout)))
       (map ConsumerRecord->map)))

(defn stream!
  "Makes a flat stream of Kafka messages.
   Commits before each poll when `:commit-before-next-poll`
   set to `true` (default is `true`)."
  ([^KafkaConsumer consumer]
   (stream consumer {}))
  ([^KafkaConsumer consumer
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
