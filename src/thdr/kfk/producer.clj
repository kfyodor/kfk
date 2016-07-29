(ns thdr.kfk.producer
  "A thin wrapper around KafkaProducer"
  (:require [thdr.kfk
             [util :as u]
             [types :refer [to-map]]]
            [schema.core :as s])
  (:import [org.apache.kafka.clients.producer KafkaProducer Producer Callback]
           [org.apache.kafka.common.serialization Serializer StringSerializer]))

(s/defschema ProducerArgs
  {(s/optional-key :key-serializer) Serializer
   (s/optional-key :value-serializer) Serializer
   :props u/PropsMap})

(defn- map->ProducerRecord
  ([^String topic record]
   (map->ProducerRecord topic {} record))
  ([^String topic
    {:keys [key
            key-fn
            partition
            timestamp
            timestamp-fn]
     :as opts}
    record]
   (let [key (or key (and key-fn (key-fn record)))
         ts  (or timestamp (and timestamp-fn (timestamp-fn record)))]
     (ProducerRecord. topic partition ts key record))))

(defn- make-send-callback [f]
  (reify Callback
    (onCompletion [_ metadata ex]
      (-> (RecordMetadata->map metadata)
          (f ex)))))

(defn kafka-producer
  "Makes an instance of KafkaProducer

   Keys `:key-serializer` and `:value-serialzier` are optional,
   since serializer classes can be passed via `:props`.

   (kafka-producer :key-serializer (StringSerializer.)
                   :value-serializer (MyValueSerializer.)
                   :props {:bootstrap.servers [\"localhost:9092\"]
                           ....etc....})
  "
  [& {:keys [^Serializer key-serializer
             ^Serializer value-serializer
             props]
      :or {key-serializer nil
           value-serializer nil}
      :as args}]
  {:pre [(s/validate ProducerArgs args)]}
  (-> (u/make-props props)
      (KafkaProducer. key-serializer value-serializer)))

(defn send!
  "Sends a record to Kafka.

   Valid options are (all are optional):
   `:key`          - record key
   `:key-fn`       - function which extracts key from record
   `:partition`    - partition number
   `:timestamp`    - record timestamp
   `:timestamp-fn` - function which extracts timestamp from record
   `:callback`     - callback (a function of record metadata map and exception)"
  ([^Producer producer ^String topic record]
   (send! producer topic record {}))
  ([^Producer producer ^String topic record {:keys [callback] :as opts}]
   (let [opts     (dissoc opts :callback)
         record   (map->ProducerRecord topic opts record)
         callback (and callback (make-send-callback callback))]
     (.send producer record callback))))

(defn flush!
  [^Producer producer]
  (.flush producer))

(defn close!
  [^Producer producer]
  (.close producer))

(defn partitions-for
  [^Producer producer ^String topic]
  (mapv PartitionInfo->map
        (.partitionsFor producer topic)))
