(ns thdr.kfk.producer-test
  (:require [thdr.kfk.producer :as p]
            [clojure.test :refer :all])
  (:import [org.apache.kafka.common.serialization StringSerializer]
           [org.apache.kafka.clients.producer
            RecordMetadata
            KafkaProducer
            MockProducer
            ProducerRecord]))

(defn test-record [val]
  {:topic "test"
   :key "key"
   :value val
   :partition 0})

(defn last-record [^MockProducer producer]
  (last (.history producer)))

(comment
  (deftest make-kafka-producer-test
    (let [producer (p/kafka-producer :key-serializer (StringSerializer.)
                                     :value-serializer (StringSerializer.)
                                     :props {:bootstrap.servers ["localhost:9092"]})]
      (is (instance? KafkaProducer producer))
      (.close producer))))

(deftest make-producer-record-test
  (testing "with partition"
    (let [ts (System/currentTimeMillis)
          record (#'p/make-producer-record (merge (test-record "value")
                                                  {:timestamp ts}))]
      (is (instance? ProducerRecord record))
      (is (= "value" (.value record)))
      (is (= "key" (.key record)))
      (is (= 0 (.partition record)))
      (is (= "test" (.topic record)))
      (is (= ts (.timestamp record)))))

  (testing "parition is not provided"
    (let [ts (System/currentTimeMillis)
          record (#'p/make-producer-record (-> (test-record "value")
                                               (merge {:timestamp ts})
                                               (dissoc :partition)))]
      (is (instance? ProducerRecord record))
      (is (= "value" (.value record)))
      (is (= "key" (.key record)))
      (is (nil? (.partition record)))
      (is (= "test" (.topic record)))
      (is (= ts (.timestamp record))))))

(deftest send-and-callbacks-test
  (let [producer (MockProducer. false (StringSerializer.) (StringSerializer.))
        data (atom {:metadata nil :error nil})
        cb (fn [metadata ex]
             (if ex
               (swap! data assoc :error ex)
               (swap! data assoc :metadata metadata)))]

    (testing "send without callback"
      (let [fut (p/send! producer (test-record "1"))]
        (.completeNext producer)

        (is (instance? RecordMetadata @fut))

        (let [record (last-record producer)]
          (is (= "key" (.key record)))
          (is (= "1" (.value record)))
          (is (= "test" (.topic record))))))

    (testing "send success"
      (p/send! producer (test-record "2") cb)
      (.completeNext producer)

      (is (= [:checksum
              :offset
              :partition
              :serialized-key-size
              :serialized-value-size
              :timestamp
              :topic]
             (keys (:metadata @data))))

      (is (nil? (:error @data)))

      (let [record (last-record producer)]
        (is (= "key" (.key record)))
        (is (= "2" (.value record)))
        (is (= "test" (.topic record)))))

    (testing "record send failed"
      (p/send! producer (test-record "fail") cb)
      (.errorNext producer (RuntimeException. "test failure"))

      (let [err (:error @data)]
        (is (instance? RuntimeException err))
        (is (= "test failure" (.getMessage err)))))

    (.close producer)))
