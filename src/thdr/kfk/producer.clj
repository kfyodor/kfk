(ns thdr.kfk.producer
  "A thin wrapper around KafkaProducer"
  (:require [thdr.kfk
             [util :as u]
             [types :refer [to-map]]]
            [schema.core :as s])
  (:import [org.apache.kafka.clients.producer
            KafkaProducer
            Producer
            Callback
            ProducerRecord]
           [org.apache.kafka.common.serialization Serializer StringSerializer]))

(s/defschema ProducerArgs
  {(s/optional-key :key-serializer) Serializer
   (s/optional-key :value-serializer) Serializer
   :props u/PropsMap})

(defn- make-producer-record
  ([{:keys [topic
            key
            value
            partition
            ^long timestamp]
     :as record}]
   {:pre [(and (not (nil? value)) (not (nil? topic)))]}
   (ProducerRecord. topic (int partition) timestamp key value)))

(defn- make-send-callback [f]
  (reify Callback
    (onCompletion [_ metadata ex]
      (if ex
        (f nil ex)
        (f (to-map metadata) nil)))))

(defn send!
  "Sends a record to Kafka.

   Valid record keys are (* is required):
   `:topic`*        - a Kafka topic this record will be sent to
   `:key`          - record key
   `:partition`    - partition number
   `:timestamp`    - record timestamp"
  ([^Producer producer record]
   (send! producer record nil))
  ([^Producer producer record callback]
   (let [record   (make-producer-record record)
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
  (mapv to-map (.partitionsFor producer topic)))

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
