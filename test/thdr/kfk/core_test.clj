(ns thdr.kfk.core-test
  (:require [thdr.kfk.core :refer [defkafkamessage]]
            [clojure.test :refer :all]))

(defkafkamessage test-event []
  :topic :test-event
  :key-fn :id
  :serialize-fn :id
  :some-key :some-value)

(defkafkamessage test-event-with-binding [a b]
  :topic :test-event
  :key-fn :id
  :serialize-fn #(merge % {:a a :b b}))

(deftest dekafkamessage-macro-test
  (testing "message without binding"
    (is (fn? make-test-event-message))
    (is (= {:topic "test_event"
            :partition nil
            :timestamp nil
            :key "id"
            :value "id"}
           (make-test-event-message {:id "id"})))

    (is (= "b" (:key (make-test-event-message {:id "id"} {:key "b"}))))
    (is (= 1 (:partition (make-test-event-message {:id "id"} {:partition 1})))))

  (testing "make-...-message*"
    (let [{:keys [some-key message]} (make-test-event-message* {:id 1})]
      (is (= :some-value some-key))
      (is (= message (make-test-event-message {:id 1})))))

  (testing "message with binding"
    (is (= (make-test-event-with-binding-message "a" "b" {:id "id"})
           {:topic "test_event"
            :partition nil
            :timestamp nil
            :key "id"
            :value {:id "id"
                    :a "a"
                    :b "b"}}))))
