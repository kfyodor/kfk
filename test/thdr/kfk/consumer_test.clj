(ns thdr.kfk.consumer-test
  (:require [thdr.kfk.consumer :as c]
            [thdr.kfk.types :refer [to-map]]
            [clojure.test :refer :all])
  (:import [org.apache.kafka.clients.consumer
            MockConsumer
            OffsetResetStrategy
            ConsumerRecord]
           [org.apache.kafka.common
            PartitionInfo
            Node]))

(defn- add-record!
  [^MockConsumer consumer
   {:keys [topic partition offset key value]
    :or {topic "test"
         partition 0
         offset 0}}]
  (.addRecord consumer (ConsumerRecord. topic partition offset key value)))

(defn add-records!
  [^MockConsumer consumer records]
  (doseq [r records]
    (add-record! consumer r)))

(deftest subscribe-test
  (let [consumer (MockConsumer. OffsetResetStrategy/NONE)]
    (is (= #{} (c/subscription consumer)))

    (c/subscribe! consumer ["test1" "test2"])
    (is (= #{"test1" "test2"} (c/subscription consumer)))

    (c/unsubscribe! consumer)
    (is (= #{} (c/subscription consumer)))

    (c/subscribe! consumer ["test1" "test2"] (fn [p]) (fn [p]))
    (is (= #{"test1" "test2"} (c/subscription consumer)))

    (c/unsubscribe! consumer)

    (c/subscribe! consumer
                  ["test1" "test2"]
                  (c/make-rebalance-listener (fn [p]) (fn [p])))
    (is (= #{"test1" "test2"} (c/subscription consumer)))
    (.close consumer)))

(deftest rebalance-listener-test
  (let [state (atom nil)
        rebalance-listener (c/make-rebalance-listener
                            (fn [p] (reset! state [:assigned p]))
                            (fn [p] (reset! state [:revoked p])))
        tp (#'c/make-TopicPartitions [{:topic "test" :partition 0}])]
    (.onPartitionsAssigned rebalance-listener tp)
    (is (= [:assigned [{:topic "test" :partition 0}]] @state))

    (.onPartitionsRevoked rebalance-listener tp)
    (is (= [:revoked [{:topic "test" :partition 0}]] @state))))

(deftest assign-test
  (let [consumer (MockConsumer. OffsetResetStrategy/NONE)]
    (c/assign! consumer [{:topic "test" :partition 1}])
    (is (= [{:topic "test" :partition 1}] (c/assignment consumer)))
    (.close consumer)))

(deftest partitions-for-and-list-topics-test
  (let [consumer (MockConsumer. OffsetResetStrategy/NONE)
        partitions (map #(PartitionInfo. "test"
                                         %
                                         (Node/noNode)
                                         (make-array Node 0)
                                         (make-array Node 0)) [0 1])]
    (.updatePartitions consumer "test" partitions)
    (is (= (mapv to-map partitions) (c/partitions-for consumer "test")))
    (is (= {"test" (mapv to-map partitions)} (c/list-topics consumer)))
    (.close consumer)))

(deftest poll-test
  (let [consumer (MockConsumer. OffsetResetStrategy/EARLIEST)]
    (c/subscribe! consumer ["test"])
    (.rebalance consumer
                (#'c/make-TopicPartitions [{:topic "test" :partition 0}]))
    (.updateBeginningOffsets consumer
                             {(#'c/make-TopicPartition "test" 0) 0})

    (add-record! consumer {:key "key1" :value "value1" :offset 0})
    (add-record! consumer {:key "key2" :value "value2" :offset 1})

    (is (= [{:key "key1"
             :value "value1"
             :offset 0
             :timestamp -1
             :partition 0
             :topic "test"}
            {:key "key2"
             :value "value2"
             :offset 1
             :timestamp -1
             :partition 0
             :topic "test"}]
           (c/poll! consumer 1)))
    (is (= 2 (c/position consumer "test" 0)))

    (c/commit-sync! consumer)

    (is (= 2 (:offset (c/committed consumer "test" 0))))

    (add-record! consumer {:key "key3" :value "value3" :offset 2})
    (is (= [{:key "key3"
              :value "value3"
              :offset 2
              :timestamp -1
              :partition 0
             :topic "test"}] (c/poll! consumer 1)))

    (is (= 3 (c/position consumer "test" 0)))

    (c/commit-sync! consumer [{:topic "test" :partition 0 :offset 3}])

    (is (= 3 (:offset (c/committed consumer "test" 0))))

    (.close consumer)))
