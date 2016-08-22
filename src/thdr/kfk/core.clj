(ns thdr.kfk.core
  (:require [camel-snake-kebab.core :refer [->snake_case ->kebab-case]]))

(defmacro defkafkamessage
  "Syntactic sugar for generating messages for Kafka.
   Sets the topic same to `event`, fetches `key` from
   provided object if `key-fn` is set and transforms a value
   with provided `serialize-fn`.

   Example:

   (defkafkamessage user-created
     :topic :user-events ;; a Kafka topic message will be sent to
     :key-fn #(-> % :id str) ;; applied to obj before serialization
     :serialize-fn #(merge % {:type :created :id (uuid-to-bytes (:id %))}))

   (make-user-created-message {:id (java.util.UUID/randomUUID)}
                              {:partition 0}) ;; optional Kafka message keys"
  [event & {:keys [topic key-fn serialize-fn deserialize-fn]
            :or {serialize-fn identity
                 deserialize-fn identity}}]
  {:pre [(not (nil? topic))]}
  (let [fn-name (symbol (str "make-" event "-message"))]
    `(defn ~fn-name
       ([obj#]
        (~fn-name obj# {}))
       ([obj# opts#]
        (let [{partition# :partition
               key# :key
               timestamp# :timestamp} opts#]
          {:topic ~(-> topic name ->snake_case)
           :partition partition#
           :timestamp timestamp#
           :key (cond key# key# ~key-fn (~key-fn obj#) :else nil)
           :value (~serialize-fn obj#)})))))
